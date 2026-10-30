//! 控制端工具：`portmap` / `ssh` / `sftp` / `tunnel`。

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::cli::args::Args;
use crate::cli::current_user;
use crate::cli::ui;
use crate::cloud::api::Api;
use crate::cloud::config;
use crate::cloud::creds::Credentials;
use crate::net::viewer::{self, rtc::FlowHandle, ssh as russh_client};

pub async fn portmap_command(args: &[String]) -> Result<(), String> {
    let args = Args::new(args);
    let device = args.require_device()?;
    let bind = args.value("--bind").unwrap_or("127.0.0.1").to_owned();
    let rules = parse_portmap_rules(&args)?;
    portmap_run_rules(device, &bind, &rules, !args.flag("--no-force-join")).await
}

fn parse_portmap_rule(spec: &str) -> Result<(u16, String, u16), String> {
    let mut parts = spec.splitn(3, ',');
    let local: u16 = parts
        .next()
        .ok_or_else(|| format!("--map 格式应为 本地端口,目标主机,目标端口: {spec}"))?
        .trim()
        .parse()
        .map_err(|_| format!("--map 本地端口无效: {spec}"))?;
    let host = parts
        .next()
        .ok_or_else(|| format!("--map 缺少目标主机: {spec}"))?
        .trim()
        .to_owned();
    let port: u16 = parts
        .next()
        .ok_or_else(|| format!("--map 缺少目标端口: {spec}"))?
        .trim()
        .parse()
        .map_err(|_| format!("--map 目标端口无效: {spec}"))?;
    if host.is_empty() {
        return Err(format!("--map 目标主机不能为空: {spec}"));
    }
    Ok((local, host, port))
}

fn parse_portmap_rules(args: &Args<'_>) -> Result<Vec<(u16, String, u16)>, String> {
    let maps: Vec<_> = args.values("--map").into_iter().map(parse_portmap_rule).collect::<Result<_, _>>()?;
    if !maps.is_empty() {
        return Ok(maps);
    }
    let positional = args.positionals();
    if positional.len() != 3 {
        return Err(
            "用法: uu-minimal portmap --device <id> [--bind 地址] <本地端口> <目标主机> <目标端口>\n\
             或多条映射（共享一条 WebRTC，可开多个本地端口）:\n\
               uu-minimal portmap --device <id> [--bind 地址] \\\n\
                 --map 2222,127.0.0.1,22 --map 3389,192.168.0.10,3389\n\
             --no-force-join 不抢占已有控制端，连接失败会等待重试；多端口请用多个 --map。"
                .into(),
        );
    }
    let local: u16 = positional[0].parse().map_err(|_| "本地端口无效".to_string())?;
    let port: u16 = positional[2].parse().map_err(|_| "目标端口无效".to_string())?;
    Ok(vec![(local, positional[1].to_owned(), port)])
}

pub async fn portmap_run(device: &str, local: u16, host: &str, port: u16) -> Result<(), String> {
    portmap_run_bind(device, "127.0.0.1", local, host, port).await
}

pub async fn portmap_run_bind(device: &str, bind: &str, local: u16, host: &str, port: u16) -> Result<(), String> {
    portmap_run_rules(device, bind, &[(local, host.to_owned(), port)], true).await
}

/// 多条本地映射共享一次 `join`；force_join=false 时等待接入，不抢占已有控制端。
/// `bind` 可选：`--bind 0.0.0.0` 让局域网其它机器也能连入。
/// WebRTC 断线自动重连，失败后从 0.5s 开始退避，最多 30s。
pub async fn portmap_run_rules(
    device: &str,
    bind: &str,
    rules: &[(u16, String, u16)],
    force_join: bool,
) -> Result<(), String> {
    if rules.is_empty() {
        return Err("至少一条端口映射".into());
    }
    use std::net::IpAddr;

    let bind_addr: IpAddr = bind
        .parse()
        .map_err(|_| format!("监听地址无效: {bind}（示例: 127.0.0.1 或 0.0.0.0）"))?;
    let device = Arc::new(device.to_owned());
    let api = Arc::new(loaded_api()?);

    // 会话槽：None = 正在重连；重连只在这里发生，accept 循环永远不退出。
    let session: Arc<tokio::sync::RwLock<Option<Arc<viewer::rtc::Peer>>>> = Arc::new(tokio::sync::RwLock::new(None));
    let session_ready = Arc::new(tokio::sync::Notify::new());

    // 后台监控：状态回调唤醒硬断线处理，探测只负责识别仍显示 Open 的假在线。
    {
        let session = session.clone();
        let api = api.clone();
        let session_ready = session_ready.clone();
        let device = device.clone();
        tokio::spawn(async move {
            loop {
                let peer = loop {
                    let ready = session_ready.notified();
                    if let Some(peer) = session.read().await.clone() { break peer; }
                    ready.await;
                };
                let mut missed_pings = 0u8;
                let period = std::time::Duration::from_secs(20);
                let mut probes = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                probes.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = peer.wait_transport_closed() => break,
                        _ = probes.tick() => {
                            match peer.probe_or_closed().await {
                                None => break,
                                Some(true) => missed_pings = 0,
                                Some(false) => {
                                    missed_pings += 1;
                                    log::warn!("链路探测未收到回包: consecutive_failures={missed_pings}");
                                }
                            }
                            if missed_pings >= 3 { break; }
                        }
                    }
                }
                *session.write().await = None;
                log::warn!("链路已断开，准备重连");
                peer.close().await;
                connect_portmap(&api, &device, &session, &session_ready, force_join).await;
            }
        });
    }

    println!("{}", ui::bold("隧道就绪"));
    for (local, host, port) in rules {
        println!("  {bind}:{local} → {host}:{port}");
    }
    if bind == "0.0.0.0" || bind == "::" {
        ui::hint("  注意: 监听所有网卡，本机所在网络的其它设备均可使用此隧道");
    }
    ui::hint("  Ctrl-C 退出；链路断开会自动重连");
    if !force_join {
        ui::hint("  非抢占模式：不顶掉已有控制端；连接失败会等待重试");
    }
    connect_portmap(&api, &device, &session, &session_ready, force_join).await;

    for (local, host, port) in rules {
        let listener = tokio::net::TcpListener::bind((bind_addr, *local))
            .await
            .map_err(|e| format!("监听 {bind}:{local} 失败: {e}"))?;
        let session = session.clone();
        let host = host.clone();
        let port = *port;
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { break };
                let session = session.clone();
                let host = host.clone();
                tokio::spawn(async move {
                    accept_portmap_stream(session, &host, port, socket).await;
                });
            }
        });
    }

    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn connect_portmap(
    api: &Api,
    device: &str,
    session: &tokio::sync::RwLock<Option<Arc<viewer::rtc::Peer>>>,
    session_ready: &tokio::sync::Notify,
    force_join: bool,
) {
    let mut backoff = crate::net::retry::Backoff::new();
    let started = std::time::Instant::now();
    let mut attempts = 0;
    loop {
        attempts += 1;
        match viewer::join(api, device, viewer::signal::CAPTURE_PORT_MAP, force_join).await {
            Ok(s) => {
                *session.write().await = Some(s.peer);
                session_ready.notify_one();
                println!("{}", ui::bold("链路已连接"));
                if attempts > 1 {
                    println!("  （本轮尝试 {attempts} 次，耗时 {}ms）", started.elapsed().as_millis());
                }
                return;
            }
            Err(e) => {
                let delay = backoff.delay();
                log::warn!("连接失败（{}ms 后重试）: {e}", delay.as_millis());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn accept_portmap_stream(
    session: Arc<tokio::sync::RwLock<Option<Arc<viewer::rtc::Peer>>>>,
    host: &str,
    port: u16,
    socket: tokio::net::TcpStream,
) {
    for _ in 0..20 {
        let peer = session.read().await.clone();
        if let Some(peer) = peer {
            match peer.open_flow(host, port).await {
                Ok(flow) => {
                    pump_socket(socket, flow).await;
                    return;
                }
                Err(e) if e.contains("not opened") => {}
                Err(e) => {
                    log::warn!("打开流失败: {e}");
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    log::warn!("连接在等待链路恢复时超时，已断开本地连接");
}

/// 内嵌 SSH（russh）：一条端口映射流直接当 socket，不碰 `~/.ssh/config`。

pub async fn ssh_command(args: &[String]) -> Result<(), String> {
    let args = Args::new(args);
    let device = args.require_device()?;
    let positional = args.positionals();
    if positional.len() > 1 {
        return Err(
            "用法: uu-minimal ssh --device <id> [--user 用户] [--port 22] [--key 私钥] [--command 命令] [主机]".into(),
        );
    }
    let (default_host, default_port) = config::ssh_target();
    let host = positional.first().map(|host| (*host).to_owned()).unwrap_or(default_host);
    let port = match args.value("--port") {
        Some(value) => value.parse::<u16>().map_err(|_| format!("--port 不是有效端口: {value}"))?,
        None => default_port,
    };
    let user = args.value("--user").map(str::to_owned).unwrap_or_else(current_user);
    ssh_session(
        device,
        &host,
        port,
        &user,
        args.value("--key").map(std::path::PathBuf::from),
        args.value("--command").map(str::to_owned),
        args.value("--password").map(str::to_owned),
        !args.flag("--no-force-join"),
    )
    .await
}

async fn ssh_session(
    device: &str,
    host: &str,
    port: u16,
    user: &str,
    key: Option<std::path::PathBuf>,
    command: Option<String>,
    password: Option<String>,
    force_join: bool,
) -> Result<(), String> {
    let user = if user.trim().is_empty() { current_user() } else { user.trim().to_owned() };
    let api = loaded_api()?;
    let session = viewer::join(&api, device, viewer::signal::CAPTURE_PORT_MAP, force_join).await?;
    let flow = session.peer.open_flow(host, port).await?;
    let known_hosts = config::path().with_file_name("known_hosts");
    russh_client::run(
        flow,
        host,
        russh_client::Options { user, port, password, key, command, known_hosts },
    )
    .await
}

/// stdio 桥：给 `ssh -o ProxyCommand=…` / `sftp -o ProxyCommand=…` 用。

pub async fn tunnel_command(args: &[String]) -> Result<(), String> {
    let args = Args::new(args);
    let device = args.require_device()?;
    let positional = args.positionals();
    if positional.len() != 2 {
        return Err("用法: uu-minimal tunnel --device <id> <主机> <端口>".into());
    }
    let host = positional[0];
    let port: u16 = positional[1].parse().map_err(|_| "端口无效".to_string())?;

    let api = loaded_api()?;
    let session = viewer::join(&api, device, viewer::signal::CAPTURE_PORT_MAP, !args.flag("--no-force-join")).await?;
    let flow = session.peer.open_flow(host, port).await?;
    let (writer, mut reader) = flow.into_parts();

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let up = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = writer.fin().await;
                    break;
                }
                Ok(n) => {
                    if writer.send(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    let down = async {
        while let Some(chunk) = reader.recv().await {
            if stdout.write_all(&chunk).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    };
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
    Ok(())
}

pub async fn sftp_command(args: &[String]) -> Result<(), String> {
    let args = Args::new(args);
    let device = args.require_device()?;
    // 其余位置参数原样交给系统 sftp（`用户@主机`、路径、-P 之类）。
    let extra: Vec<String> = args.positionals().into_iter().map(str::to_owned).collect();
    spawn_sftp(device, &extra, !args.flag("--no-force-join")).await
}

fn sftp_proxy(exe: &std::path::Path, device: &str, force_join: bool) -> String {
    let mode = if force_join { "" } else { " --no-force-join" };
    format!("{} tunnel --device {}{mode} %h %p", exe.display(), device)
}

async fn spawn_sftp(device: &str, extra: &[String], force_join: bool) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| format!("找不到自身路径: {e}"))?;
    let proxy = sftp_proxy(&exe, device, force_join);
    let mut command = tokio::process::Command::new("sftp");
    command.arg("-o").arg(format!("ProxyCommand={proxy}")).args(extra);
    let status = command.status().await.map_err(|e| format!("启动 sftp 失败: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("sftp 退出码 {}", status.code().unwrap_or(-1)))
    }
}

async fn pump_socket(socket: tokio::net::TcpStream, flow: FlowHandle) {
    let (writer, mut reader) = flow.into_parts();
    let (mut read, mut write) = socket.into_split();
    let up = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            match read.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = writer.fin().await;
                    break;
                }
                Ok(n) => {
                    if writer.send(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    let down = async {
        while let Some(chunk) = reader.recv().await {
            if write.write_all(&chunk).await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
}

pub fn loaded_api() -> Result<Api, String> {
    let file = config::read()?;
    Ok(Api::new(Credentials::load(file.as_ref())?))
}

// ---------------------------------------------------------------- 命令行参数


pub async fn exec_ssh(device: &str, host: &str, port: u16, user: &str) -> Result<(), String> {
    ssh_session(device, host, port, user, None, None, None, true).await
}

pub async fn exec_sftp(device: &str, host: &str, user: &str, path: &str) -> Result<(), String> {
    let target = if user.trim().is_empty() { host.to_owned() } else { format!("{}@{}", user.trim(), host) };
    let target = if path.trim().is_empty() { target } else { format!("{target}:{}", path.trim()) };
    spawn_sftp(device, &[target], true).await
}

#[cfg(test)]
mod tests;
