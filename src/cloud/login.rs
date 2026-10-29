//! 扫码登录，与官方客户端同一条链路：
//!   1. POST /api/v1/device/{macos|windows}/init  匿名注册本机，得到 device_id（平台见配置 `platform`）
//!   2. POST /api/v1/qrcode/gen/login      -> qrcode_jump_url（二维码内容）+ status_query_ticket
//!   3. POST /api/v1/qrcode/login/status   长轮询，login_status 1..4
//!   4. POST /api/v1/login/by_qrcode       -> user_id + token（csrf 在 JWT claim 里）
//!
//! 二维码内容等价于登录票据：只打印到当前终端，不写日志、不落盘。

use crate::cloud::api::Api;
use crate::cloud::creds::{Credentials, DEFAULT_SIGN_KEY};
use serde_json::{json, Value};
use std::time::Duration;

const POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// 二维码失效（HTTP 400 code 1190）。
const QR_EXPIRED: i64 = 1190;

pub async fn run() -> Result<(), String> {
    let mut cfg = crate::cloud::config::read()?.unwrap_or_else(|| json!({}));
    let mut creds = ensure_identity(&mut cfg)?;
    crate::cloud::config::write(&cfg)?;

    if creds.device_id.is_empty() {
        let platform = crate::cloud::config::platform();
        let body = init_body(platform, &cfg, &creds);
        let init_path = format!("/api/v1/device/{}/init", platform.slug());
        let (status, resp) = Api::new(creds.clone()).post(&init_path, Some(&body)).await?;
        let data = unwrap_data("device init", status, &resp)?;
        let id = data.get("device_id").and_then(Value::as_str).ok_or("device init 没有返回 device_id")?.to_owned();
        cfg["device_id"] = json!(id);
        let path = crate::cloud::config::write(&cfg)?;
        creds.device_id = id;
        log::info!("已按 {} 平台注册本机设备身份，凭据写入 {}", platform.slug(), path.display());
    } else {
        log::info!(
            "已有设备身份，跳过注册（当前平台 {}）；要以该平台重新注册，删掉配置里的 device_id 后再登录",
            crate::cloud::config::platform().slug()
        );
    }

    let api = Api::new(creds);
    'outer: loop {
        let (status, resp) = match api.post_with_timeout("/api/v1/qrcode/gen/login", None, POLL_TIMEOUT).await {
            Ok(v) => v,
            Err(e) => {
                println!("二维码获取失败：{e}；3 秒后重试");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let d = match unwrap_data("qrcode gen", status, &resp) {
            Ok(d) => d,
            Err(e) => {
                println!("二维码获取失败：{e}；3 秒后重试");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let url = d.get("qrcode_jump_url").and_then(Value::as_str).ok_or("二维码接口没有返回 qrcode_jump_url")?.to_owned();
        // 当前云端用 status_query_ticket；老服务端只有 token 字段。
        let ticket = d
            .get("status_query_ticket")
            .or_else(|| d.get("token"))
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .ok_or("二维码接口没有返回查询票据")?
            .to_owned();

        println!("\n用 UU 远程手机 App 扫描下面的二维码（二维码等同凭据，不要截图或转发）：\n");
        println!("{}", render_qr(&url)?);

        let mut last_status = 1i64;
        loop {
            let query = json!({ "token": &ticket, "qrcode_jump_url": &url, "login_status": last_status });
            let (status, resp) = match api.post_with_timeout("/api/v1/qrcode/login/status", Some(&query), POLL_TIMEOUT).await {
                Ok(v) => v,
                Err(_) => {
                    println!("查询超时，重新生成二维码");
                    continue 'outer;
                }
            };
            if resp.get("code").and_then(Value::as_i64) == Some(QR_EXPIRED) {
                println!("二维码已失效，重新生成");
                continue 'outer;
            }
            let d = match unwrap_data("qrcode status", status, &resp) {
                Ok(d) => d,
                Err(e) => {
                    println!("查询失败：{e}，重新生成二维码");
                    continue 'outer;
                }
            };
            let state = d.get("status").and_then(Value::as_i64).unwrap_or(last_status);
            match state {
                1 => {}
                2 => {
                    if last_status != 2 {
                        println!("已扫码，请在手机上确认");
                    }
                }
                3 => {
                    println!("二维码过期，重新生成");
                    continue 'outer;
                }
                0 => {
                    // 官方客户端在 0 之后 350ms 立刻重新申请。
                    tokio::time::sleep(Duration::from_millis(350)).await;
                    continue 'outer;
                }
                4 => {
                    let body = json!({ "token": &ticket, "qrcode_jump_url": &url });
                    let (status, resp) = match api.post_with_timeout("/api/v1/login/by_qrcode", Some(&body), POLL_TIMEOUT).await {
                        Ok(v) => v,
                        Err(e) => {
                            println!("登录兑换失败：{e}，重新生成二维码");
                            continue 'outer;
                        }
                    };
                    let d = match unwrap_data("qrcode login", status, &resp) {
                        Ok(d) => d,
                        Err(e) => {
                            println!("登录未获批准：{e}，重新生成二维码");
                            continue 'outer;
                        }
                    };
                    let token = d.get("token").and_then(Value::as_str).ok_or("登录接口没有返回 token")?;
                    let user = d.get("user_id").and_then(Value::as_str).ok_or("登录接口没有返回 user_id")?;
                    cfg["token"] = json!(token);
                    cfg["user_id"] = json!(user);
                    cfg["csrf"] = json!(csrf_from_token(token).ok_or("token 里没有 csrf claim")?);
                    if let Some(nickname) = d.get("nickname").and_then(Value::as_str).filter(|n| !n.is_empty()) {
                        cfg["account_name"] = json!(nickname);
                        println!("登录成功：{nickname}");
                    } else {
                        println!("登录成功");
                    }
                    let path = crate::cloud::config::write(&cfg)?;
                    println!("凭据已保存到 {}（权限 0600），之后直接运行 `uu-minimal run`", path.display());
                    return Ok(());
                }
                other => {
                    println!("未知的二维码状态 {other}，重新生成");
                    continue 'outer;
                }
            }
            last_status = state;
        }
    }
}

fn unwrap_data<'a>(what: &str, status: u16, body: &'a Value) -> Result<&'a Value, String> {
    let code = body.get("code").and_then(Value::as_i64).unwrap_or(-1);
    if status == 200 && code == 0 {
        return Ok(body.get("data").unwrap_or(&Value::Null));
    }
    Err(format!("{what} 失败: http {status} code {code}"))
}

/// 配置项：`UU_<KEY>` 环境变量优先，其次 config.json 同名字段，都没有则空串。
fn setting(cfg: &Value, key: &str) -> String {
    std::env::var(format!("UU_{}", key.to_uppercase()))
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| cfg.get(key).and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default()
}

fn setting_or(cfg: &Value, key: &str, default: &str) -> String {
    let value = setting(cfg, key);
    if value.is_empty() { default.to_owned() } else { value }
}

/// 注册 body。macOS 与 Windows 的字段集不同：macOS 额外要
/// `system_version / model / model_identifier / system_name / resolution / dpi`，
/// 且 `memory` 必须是字符串（缺一个服务端就 422 1001）。这些值都可用 config.json 同名项覆盖。
fn init_body(platform: crate::cloud::config::Platform, cfg: &Value, creds: &Credentials) -> Value {
    let ram = memory_mb();
    let mut body = json!({
        "name": setting(cfg, "host_name"),
        "client_id": &creds.client_id,
        "system_id": &creds.system_id,
        "machine_guid": setting(cfg, "machine_guid"),
        "os": os_name(),
        "base_board": base_board(),
        "cpu": cpu_name(),
        "video": video(),
        "mac": mac(),
        "memory": ram,
        "screen": "1920x1080",
    });
    if platform == crate::cloud::config::Platform::Mac {
        body["memory"] = json!(ram.to_string());
        body["system_version"] = json!(setting_or(cfg, "system_version", "14.5"));
        body["model"] = json!(setting_or(cfg, "model", "MacBook Pro"));
        body["model_identifier"] = json!(setting_or(cfg, "model_identifier", "MacBookPro18,3"));
        body["system_name"] = json!(setting_or(cfg, "system_name", "macOS"));
        body["resolution"] = json!(setting_or(cfg, "resolution", "1920x1080"));
        body["dpi"] = json!(setting(cfg, "dpi").parse::<u32>().unwrap_or(96));
    }
    body
}

/// 稳定的本机标识：登录一次后复用，账号里始终是同一台设备。
/// `machine_guid` 与设备名 `host_name` 都从配置读，缺失时才用本机值补一次并写回。
fn ensure_identity(cfg: &mut Value) -> Result<Credentials, String> {
    let makers: [(&str, fn() -> String); 3] = [
        ("client_id", random_uuid),
        ("system_id", || random_uuid().to_uppercase()),
        ("machine_guid", machine_guid),
    ];
    for (key, make) in makers {
        if setting(cfg, key).is_empty() {
            cfg[key] = json!(make());
        }
    }
    if setting(cfg, "host_name").is_empty() {
        cfg["host_name"] = json!(hostname());
    }
    Ok(Credentials {
        client_id: setting(cfg, "client_id"),
        system_id: setting(cfg, "system_id"),
        device_id: setting(cfg, "device_id"),
        user_id: setting(cfg, "user_id"),
        token: setting(cfg, "token"),
        csrf: setting(cfg, "csrf"),
        sign_key: Some(setting(cfg, "sign_key")).filter(|k| !k.is_empty()).unwrap_or_else(|| DEFAULT_SIGN_KEY.into()),
    })
}

fn csrf_from_token(token: &str) -> Option<String> {
    use base64::Engine;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice::<Value>(&bytes).ok()?.get("csrf")?.as_str().map(str::to_owned)
}

fn render_qr(content: &str) -> Result<String, String> {
    let code = qrcode::QrCode::new(content.as_bytes()).map_err(|e| format!("QR 编码失败: {e}"))?;
    Ok(code
        .render::<qrcode::render::unicode::Dense1x2>()
        .quiet_zone(true)
        .dark_color(qrcode::render::unicode::Dense1x2::Light)
        .light_color(qrcode::render::unicode::Dense1x2::Dark)
        .build())
}

fn random_uuid() -> String {
    use std::io::Read;
    let mut b = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut b);
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

/// /etc/machine-id 转 GUID，Linux 上最接近 Windows MachineGuid 的东西。
fn machine_guid() -> String {
    let id = read("/etc/machine-id");
    if id.len() == 32 && id.bytes().all(|c| c.is_ascii_hexdigit()) {
        format!("{}-{}-{}-{}-{}", &id[..8], &id[8..12], &id[12..16], &id[16..20], &id[20..])
    } else {
        random_uuid()
    }
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).map(|s| s.trim().to_owned()).unwrap_or_default()
}

fn hostname() -> String {
    let h = read("/proc/sys/kernel/hostname");
    if h.is_empty() { "Linux".into() } else { h }
}

fn os_name() -> String {
    read("/etc/os-release")
        .lines()
        .find_map(|l| l.strip_prefix("PRETTY_NAME="))
        .map(|v| v.trim_matches('"').to_owned())
        .unwrap_or_else(|| "Linux".into())
}

fn cpu_name() -> String {
    read("/proc/cpuinfo")
        .lines()
        .find_map(|l| l.strip_prefix("model name").and_then(|r| r.split_once(':')).map(|(_, v)| v.trim().to_owned()))
        .unwrap_or_else(|| std::env::consts::ARCH.into())
}

fn base_board() -> String {
    let v = [read("/sys/class/dmi/id/board_vendor"), read("/sys/class/dmi/id/board_name")].join(" ");
    if v.trim().is_empty() { "Unknown".into() } else { v.trim().to_owned() }
}

fn video() -> Vec<String> {
    let mut list = Vec::new();
    if let Ok(dir) = std::fs::read_dir("/sys/class/drm") {
        for entry in dir.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("card") && !name.contains('-') {
                let dev = entry.path().join("device");
                list.push(format!(
                    "PCI {}:{}",
                    read(&dev.join("vendor").to_string_lossy()).trim_start_matches("0x"),
                    read(&dev.join("device").to_string_lossy()).trim_start_matches("0x")
                ));
            }
        }
    }
    if list.is_empty() {
        list.push("Unknown".into());
    }
    list
}

/// 第一块物理网卡的 MAC，大写短横线形式（与 Windows 一致）。
fn mac() -> String {
    let mut names: Vec<String> = std::fs::read_dir("/sys/class/net")
        .map(|d| {
            d.flatten()
                .filter(|e| e.path().join("device").exists())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
        .iter()
        .map(|n| read(&format!("/sys/class/net/{n}/address")))
        .find(|m| !m.is_empty() && m != "00:00:00:00:00:00")
        .map(|m| m.to_uppercase().replace(':', "-"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{init_body, setting};
    use crate::cloud::creds::Credentials;
    use crate::cloud::config::Platform;

    #[test]
    fn identity_fields_are_read_from_config() {
        // `_test_*` 前缀不会出现在真实配置里，环境变量也不会是 UU__TEST_*。
        let cfg = serde_json::json!({"_test_host_name": "box", "_test_machine_guid": "guid"});
        assert_eq!(setting(&cfg, "_test_host_name"), "box");
        assert_eq!(setting(&cfg, "_test_machine_guid"), "guid");
        assert_eq!(setting(&cfg, "_test_absent"), "");
    }

    #[test]
    fn mac_init_body_carries_the_fields_the_server_demands() {
        let cfg = serde_json::json!({"host_name": "box", "machine_guid": "guid"});
        let creds = Credentials::default();
        let mac = init_body(Platform::Mac, &cfg, &creds);
        assert_eq!(mac["name"], "box");
        assert_eq!(mac["machine_guid"], "guid");
        assert!(mac["memory"].is_string(), "macOS init 要求 memory 是字符串");
        for key in ["system_version", "model", "model_identifier", "system_name", "resolution", "dpi"] {
            assert!(!mac[key].is_null(), "{key} 缺失");
        }
        // Windows 版没有这些 macOS 专属字段，memory 仍是数字。
        let win = init_body(Platform::Windows, &cfg, &creds);
        assert!(win["system_version"].is_null());
        assert!(win["memory"].is_u64());
    }
}

fn memory_mb() -> u64 {
    read("/proc/meminfo")
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}
