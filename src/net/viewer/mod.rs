//! viewer 侧：join 房间 → `control` 拿 ICE → offer/answer → 打开通道。
//! `ssh` / `sftp` / `portmap` 都建立在这上面。

pub mod rtc;
pub mod signal;
pub mod ssh;

use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::cloud::api::Api;
use rtc::Peer;
use signal::Signal;

pub struct Session {
    /// 只要它活着，信令连接就不会被回收（事件泵在后台用它投递 soac）。
    #[allow(dead_code)]
    pub signal: Arc<Signal>,
    pub peer: Arc<Peer>,
}

/// 设备鉴权 + 建房 + 信令 + offer/answer，直到 FILE 通道可用。
pub async fn join(api: &Api, device: &str, capture_type: u32, force_join: bool) -> Result<Session, String> {
    let room = api.join_room(device, force_join).await.map_err(|e| e.to_string())?;
    let server = room.server().to_owned();
    if server.is_empty() || room.token.is_empty() {
        return Err("房间信息不完整（缺信令地址或票据）".into());
    }
    let (events, mut event_rx) = mpsc::channel::<Value>(256);
    let signal = Signal::connect(&server, &room.token, Duration::from_secs(10), events).await?;

    let app = uuid();
    let ack = signal.control(&app, device, capture_type).await?;
    let remote = ack
        .get("client_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("control ack 缺少 client_id（对端路由）")?
        .to_owned();
    // 官方云端在 ack 里指派 ice_id，不回显我们给的那个。
    let ice = ack
        .get("ice_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .map(str::to_owned)
        .unwrap_or_else(|| random_hex(16));
    let config = rtc::configuration(&ack)?;
    let peer = Arc::new(Peer::new(config, app, ice, remote, signal.clone()).await?);

    let offer = peer.offer().await?;
    signal.soac(offer).await?;
    {
        let peer = Arc::downgrade(&peer);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let Some(peer) = peer.upgrade() else { break };
                let payload = soac_payload(event);
                if let Err(e) = peer.receive(&payload).await {
                    log::warn!("处理信令事件失败: {e}");
                }
            }
        });
    }
    peer.wait_channel(rtc::FILE_CHANNEL, Duration::from_secs(20)).await?;
    Ok(Session { signal, peer })
}

/// Socket.IO 事件 `["soac", {...}]`：事件名在 [0]，answer / 候选在 [1]；其它事件原样返回。
fn soac_payload(event: Value) -> Value {
    match event {
        Value::Array(items) if items.first().and_then(Value::as_str) == Some("soac") => {
            items.get(1).cloned().unwrap_or(Value::Null)
        }
        other => other,
    }
}

pub fn uuid() -> String {
    let mut bytes = [0u8; 16];
    random_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = hex::encode(bytes);
    format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..])
}

pub fn random_hex(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    random_bytes(&mut bytes);
    hex::encode(bytes)
}

fn random_bytes(out: &mut [u8]) {
    use std::io::Read;
    if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
        let _ = file.read_exact(out);
    }
}

#[cfg(test)]
mod tests {
    use super::soac_payload;
    use serde_json::json;

    #[test]
    fn soac_events_are_unwrapped_but_other_events_are_kept() {
        let soac = json!(["soac", {"data": {"type": "answer", "sdp": "v=0"}}]);
        assert_eq!(soac_payload(soac), json!({"data": {"type": "answer", "sdp": "v=0"}}));
        // 没有第二个元素时不 panic。
        assert_eq!(soac_payload(json!(["soac"])), serde_json::Value::Null);
        // released / left 这类事件原样交给上层。
        let released = json!(["released", {"reason": "bye"}]);
        assert_eq!(soac_payload(released.clone()), released);
    }
}
