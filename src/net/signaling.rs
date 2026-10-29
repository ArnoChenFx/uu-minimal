//! Engine.IO v4 / Socket.IO 最小客户端，只处理 UU 房间需要的报文：
//!   `be-controlled`（+ 二进制 ConnectOptions）、`soac`（offer/answer/candidate）、
//!   `released` / `left`。
//! 认证靠 HTTP 头 `X-NRD-AUTH: <房间票据>`，命名空间固定 `/`。

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::net::ice::SessionIce;

mod refresh;

const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_PING_WINDOW: Duration = Duration::from_secs(45);

/// `soac` 内容。`type` = offer / answer / candidate。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Soac {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ice_id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdp_mid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdp_mline_index: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_control_id: Option<Value>,
}

pub enum Event {
    /// Socket.IO 握手完成（已发 room_info）。
    Connected,
    /// `be-controlled` 的二进制附件解出来的接单请求：capture_type + 该 peer 的 ICE 配置。
    Connect { capture_type: u64, ice_id: Option<String>, ice: SessionIce },
    Offer(Soac),
    Candidate(Soac),
    /// 遥控端离开 / 房间被回收。
    Released(String),
    /// 连接已断，需要重新建房。
    Closed,
}

pub struct Signaling {
    tx: mpsc::Sender<String>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl Drop for Signaling {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// 无法识别 peer 的离开通知按房间退出处理，重新申请房间。
fn release_event(name: &str, data: &Value) -> Event {
    let nested = data.get("data").unwrap_or(&Value::Null);
    let id = [data, nested].into_iter().flat_map(|body| ["ice_id", "iceId"].into_iter()
        .filter_map(move |key| body.get(key))).find_map(|value| match value {
        Value::String(id) if !id.is_empty() => Some(id.clone()),
        Value::Number(id) => Some(id.to_string()),
        _ => None,
    });
    log::info!("云端离开事件: event={name} ice_id_present={}", id.is_some());
    match id {
        Some(id) => Event::Released(id),
        None => Event::Closed,
    }
}

impl Signaling {
    pub fn shutdown(&self) {
        for task in &self.tasks { task.abort(); }
    }

    pub async fn connect(url: &str, room_token: &str, events: mpsc::Sender<Event>) -> Result<Arc<Self>, String> {
        Self::connect_with_timing(url, room_token, events, refresh::Timing::default()).await
    }

    async fn connect_with_timing(url: &str, room_token: &str, events: mpsc::Sender<Event>, timing: refresh::Timing) -> Result<Arc<Self>, String> {
        let ws_url = format!("{}/socket.io/?transport=websocket&EIO=4", url.trim_end_matches('/'));
        let mut request = ws_url.into_client_request().map_err(|_| "信令 URL 无效")?;
        request
            .headers_mut()
            .insert("X-NRD-AUTH", room_token.parse().map_err(|_| "房间票据无法放进请求头")?);

        let (socket, _) = tokio::time::timeout(SEND_TIMEOUT, connect_async(request)).await
            .map_err(|_| "信令 WebSocket 连接超时")?
            .map_err(|_| "信令 WebSocket 连接失败")?;
        let (mut write, mut read) = socket.split();
        let (tx, mut rx) = mpsc::channel::<String>(32);

        let writer = tokio::spawn(async move {
            while let Some(packet) = rx.recv().await {
                match tokio::time::timeout(SEND_TIMEOUT, write.send(Message::Text(packet))).await {
                    Ok(Ok(())) => {}
                    _ => break,
                }
            }
        });

        let sender = tx.clone();
        let reader = tokio::spawn(async move {
            let mut counter = 10u64;
            let mut refresh = refresh::Refresh::new(timing);
            // 45/46 的文本头先到，二进制附件随后单独一帧。
            let mut pending: Option<(String, Value)> = None;
            let mut window = DEFAULT_PING_WINDOW;
            let mut deadline = tokio::time::Instant::now() + window;
            loop {
                let message = tokio::select! {
                    _ = events.closed() => break,
                    _ = tokio::time::sleep_until(deadline) => {
                        log::warn!("信令心跳超时（未收到 ping）");
                        let _ = events.send(Event::Closed).await;
                        break;
                    }
                    _ = tokio::time::sleep_until(refresh.next.unwrap_or(deadline)), if refresh.next.is_some() => {
                        counter += 1;
                        // Socket.IO 有 ACK 编号，但事件没有额外参数。
                        if sender.try_send(format!("42{counter}[\"refresh_reconnect_key\"]")).is_err() {
                            log::warn!("重连凭证刷新请求无法入队，重建房间");
                            break;
                        }
                        refresh.sent(counter, tokio::time::Instant::now());
                        log::info!("已请求刷新房间重连凭证");
                        continue;
                    }
                    message = read.next() => message,
                };
                let Some(Ok(message)) = message else {
                    let _ = events.send(Event::Closed).await;
                    break;
                };
                match message {
                    Message::Text(text) => {
                        if text.starts_with('0') {
                            if let Ok(open) = serde_json::from_str::<Value>(&text[1..]) {
                                if let (Some(interval), Some(timeout)) = (
                                    open.get("pingInterval").and_then(Value::as_u64),
                                    open.get("pingTimeout").and_then(Value::as_u64),
                                ) {
                                    window = Duration::from_millis(interval.saturating_add(timeout).clamp(5_000, 300_000));
                                    deadline = tokio::time::Instant::now() + window;
                                }
                            }
                            let _ = sender.try_send("40".to_owned());
                        } else if text == "2" {
                            deadline = tokio::time::Instant::now() + window;
                            if sender.try_send("3".to_owned()).is_err() {
                                break;
                            }
                        } else if text.starts_with("40") {
                            refresh.connected(tokio::time::Instant::now());
                            counter += 1;
                            let _ = sender.try_send(format!("42{counter}[\"room_info\"]"));
                            log::info!("Socket.IO 已连接，等待遥控端");
                            if events.send(Event::Connected).await.is_err() {
                                break;
                            }
                        } else if text.starts_with("43") {
                            let begin = text.find('[').unwrap_or(2);
                            if let Ok(id) = text[2..begin].parse::<u64>() {
                                // 非法 JSON 也不能误判为成功；保留请求时安排的重试。
                                let payload = serde_json::from_str::<Value>(&text[begin..]).unwrap_or(Value::Null);
                                if let Some(success) = refresh.acknowledge(id, &payload, tokio::time::Instant::now()) {
                                    if success { log::info!("房间重连凭证刷新成功"); }
                                    else { log::warn!("房间重连凭证刷新应答无效，将按计划重试"); }
                                } else { log::debug!("收到非刷新或已过期的 ack id={id}"); }
                            }
                        } else if text.starts_with("45") || text.starts_with("46") {
                            let body = text.trim_start_matches(|c: char| c.is_ascii_digit() || c == '-');
                            let parsed = serde_json::from_str::<Value>(body).ok();
                            let name = parsed
                                .as_ref()
                                .and_then(|v| v.get(0))
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned();
                            let header = parsed.as_ref().and_then(|v| v.get(1)).cloned().unwrap_or(Value::Null);
                            pending = Some((name, header));
                        } else if let Some(rest) = text.strip_prefix("42") {
                            let Some(Value::Array(items)) = serde_json::from_str::<Value>(rest).ok() else { continue };
                            let name = items.first().and_then(Value::as_str).unwrap_or_default();
                            let data = items.get(1).cloned().unwrap_or(Value::Null);
                            match name {
                                "soac" => {
                                    // 外层包 {client_id, data:{...}}，老格式可能只有内层。
                                    let inner = data.get("data").cloned().unwrap_or_else(|| data.clone());
                                    let Ok(mut soac) = serde_json::from_value::<Soac>(inner) else {
                                        log::warn!("soac 解析失败");
                                        continue;
                                    };
                                    if soac.client_id.is_none() {
                                        soac.client_id = data.get("client_id").and_then(Value::as_str).map(str::to_owned);
                                    }
                                    let event = match soac.kind.as_str() {
                                        "offer" => Event::Offer(soac),
                                        "candidate" => Event::Candidate(soac),
                                        other => {
                                            log::debug!("忽略 soac type={other}");
                                            continue;
                                        }
                                    };
                                    if events.send(event).await.is_err() {
                                        break;
                                    }
                                }
                                "released" | "left" | "leave" => {
                                    let event = release_event(name, &data);
                                    let room_closed = matches!(event, Event::Closed);
                                    if events.send(event).await.is_err() || room_closed {
                                        break;
                                    }
                                }
                                other => log::debug!("忽略事件 {other}"),
                            }
                        }
                    }
                    Message::Binary(bytes) => {
                        // 新版 Engine.IO 附件是裸 protobuf，老 SDK 带 0x04 前缀。
                        let data: &[u8] = if bytes.first() == Some(&4) { &bytes[1..] } else { &bytes };
                        let Some((name, header)) = pending.take() else { continue };
                        if name != "be-controlled" {
                            continue;
                        }
                        // ConnectOptions 字段 1 = capture_type：
                        // 0 unknown, 1 desktop, 2 window, 5 文件, 8 终端, 9 端口映射。
                        let capture_type = protobuf_varint(data, 1).unwrap_or(0);
                        let ice_id = ["ice_id", "iceId"]
                            .iter()
                            .find_map(|k| header.get(*k).or_else(|| header.get("data").and_then(|d| d.get(*k))))
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        log::info!("连接请求: capture_type={capture_type} ice_id_present={}", ice_id.is_some());
                        let event = Event::Connect { capture_type, ice_id, ice: SessionIce::from_header(&header) };
                        if events.send(event).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => {
                        log::warn!("信令被云端关闭");
                        let _ = events.send(Event::Closed).await;
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(Arc::new(Self { tx, tasks: vec![writer.abort_handle(), reader.abort_handle()] }))
    }

    pub async fn emit(&self, packet: String) -> Result<(), String> {
        tokio::time::timeout(SEND_TIMEOUT, self.tx.send(packet))
            .await
            .map_err(|_| "信令发送超时".to_string())?
            .map_err(|_| "信令已断开".to_string())
    }

    pub async fn emit_soac(&self, soac: &Soac) -> Result<(), String> {
        let payload = match &soac.client_id {
            Some(client_id) => serde_json::json!({ "client_id": client_id, "data": soac }).to_string(),
            None => serde_json::to_string(soac).map_err(|e| e.to_string())?,
        };
        self.emit(format!("42[\"soac\",{payload}]")).await
    }
}

/// 只取顶层某个 varint 字段，其余按 wire type 跳过。用于 ConnectOptions.capture_type。
fn protobuf_varint(bytes: &[u8], wanted: u32) -> Option<u64> {
    let mut i = 0usize;
    while i < bytes.len() {
        let (key, next) = read_varint(bytes, i)?;
        i = next;
        match (key & 7) as u8 {
            0 => {
                let (value, next) = read_varint(bytes, i)?;
                i = next;
                if (key >> 3) as u32 == wanted {
                    return Some(value);
                }
            }
            1 => i = i.checked_add(8)?,
            2 => {
                let (len, next) = read_varint(bytes, i)?;
                i = next.checked_add(usize::try_from(len).ok()?)?;
            }
            5 => i = i.checked_add(4)?,
            _ => return None,
        }
        if i > bytes.len() {
            return None;
        }
    }
    None
}

fn read_varint(bytes: &[u8], mut i: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(i)?;
        i += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refresh_request(message: Message) -> u64 {
        let text = message.into_text().unwrap();
        let rest = text.strip_prefix("42").unwrap();
        let begin = rest.find('[').unwrap();
        assert_eq!(serde_json::from_str::<Value>(&rest[begin..]).unwrap(), serde_json::json!(["refresh_reconnect_key"]));
        rest[..begin].parse().unwrap()
    }

    #[tokio::test]
    async fn refresh_protocol_handles_success_failure_silence_and_heartbeats() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let timing = refresh::Timing { first: Duration::from_millis(50), retry: Duration::from_millis(60), success: Duration::from_millis(180) };
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            // WebSocket 建立、Engine.IO open 均不能提前启动 Socket.IO 的刷新。
            assert!(tokio::time::timeout(Duration::from_millis(80), socket.next()).await.is_err());
            socket.send(Message::Text("0{\"pingInterval\":5000,\"pingTimeout\":5000}".into())).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "40");
            assert!(tokio::time::timeout(Duration::from_millis(80), socket.next()).await.is_err());
            socket.send(Message::Text("40{}".into())).await.unwrap();
            let info = socket.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(info, "4211[\"room_info\"]");

            let first = refresh_request(socket.next().await.unwrap().unwrap());
            assert_eq!(first, 12); // 与 room_info 共用递增 ACK 编号，不能冲突。
            socket.send(Message::Text("2".into())).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "3");
            let accepted = tokio::time::Instant::now();
            socket.send(Message::Text(format!("43{first}[\"success\",{{\"reconnect_key\":\"test-only\"}}]"))).await.unwrap();
            let second = refresh_request(socket.next().await.unwrap().unwrap());
            assert!(accepted.elapsed() >= timing.success - Duration::from_millis(5));
            assert!(second > first);
            socket.send(Message::Text(format!("43{second}[\"success\",{{\"reconnect_key\":123}}]"))).await.unwrap();
            let third = refresh_request(socket.next().await.unwrap().unwrap());
            assert!(third > second);
            // 无 ACK 仍须重试，且不能妨碍收取房间退出事件。
            let fourth = refresh_request(socket.next().await.unwrap().unwrap());
            assert!(fourth > third);
            socket.send(Message::Text("42[\"leave\",{}]".into())).await.unwrap();
        });
        let (tx, mut rx) = mpsc::channel(8);
        let signal = Signaling::connect_with_timing(&format!("ws://{address}"), "test", tx, timing).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            assert!(matches!(rx.recv().await, Some(Event::Connected)));
            assert!(matches!(rx.recv().await, Some(Event::Closed)));
            server.await.unwrap();
        }).await.unwrap();
        drop(signal);
    }

    #[test]
    fn room_leave_without_peer_id_requires_reconnection() {
        for name in ["released", "left", "leave"] {
            for data in [serde_json::json!({}), serde_json::json!({"ice_id":""}), Value::Null] {
                assert!(matches!(release_event(name, &data), Event::Closed));
            }
        }
        for data in [serde_json::json!({"ice_id":"42"}), serde_json::json!({"iceId":42})] {
            assert!(matches!(release_event("left", &data), Event::Released(id) if id == "42"));
        }
    }

    #[test]
    fn nested_peer_ids_do_not_trigger_room_reconnection() {
        for name in ["released", "left", "leave"] {
            for data in [
                serde_json::json!({"data":{"ice_id":"nested"}}),
                serde_json::json!({"data":{"iceId":"nested"}}),
                serde_json::json!({"ice_id":"", "data":{"ice_id":"nested"}}),
                serde_json::json!({"ice_id":null, "data":{"iceId":"nested"}}),
            ] {
                assert!(matches!(release_event(name, &data), Event::Released(id) if id == "nested"));
            }
            assert!(matches!(release_event(name, &serde_json::json!({"data":{"iceId":42}})),
                Event::Released(id) if id == "42"));
            assert!(matches!(release_event(name, &serde_json::json!({"ice_id":"top", "data":{"ice_id":"nested"}})),
                Event::Released(id) if id == "top"));
            for data in [serde_json::json!({"data":{}}), serde_json::json!({"data":{"ice_id":""}})] {
                assert!(matches!(release_event(name, &data), Event::Closed));
            }
        }
    }

    #[tokio::test]
    async fn server_room_leave_ends_old_signaling_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.send(Message::Text("0{\"pingInterval\":25000,\"pingTimeout\":20000}".into())).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "40");
            socket.send(Message::Text("40{}".into())).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap(); // room_info
            socket.send(Message::Text("42[\"released\",{\"ice_id\":\"peer\"}]".into())).await.unwrap();
            socket.send(Message::Text("42[\"left\",{}]".into())).await.unwrap();
            // 服务端不主动断 WebSocket，客户端仍必须识别房间退出。
            let closed = tokio::time::timeout(Duration::from_secs(2), socket.next()).await.unwrap();
            assert!(!matches!(closed, Some(Ok(Message::Text(_)))));
        });
        let (tx, mut rx) = mpsc::channel(8);
        let signal = Signaling::connect(&format!("ws://{address}"), "test", tx).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            assert!(matches!(rx.recv().await, Some(Event::Connected)));
            assert!(matches!(rx.recv().await, Some(Event::Released(id)) if id == "peer"));
            assert!(matches!(rx.recv().await, Some(Event::Closed)));
            assert!(rx.recv().await.is_none());
        }).await.unwrap();
        drop(signal);
        server.await.unwrap();
    }

    #[test]
    fn reads_field_one_varint_and_skips_other_fields() {
        // field 3 (string "x"), field 1 (varint 8)
        let bytes = [0x1a, 0x01, b'x', 0x08, 0x08];
        assert_eq!(protobuf_varint(&bytes, 1), Some(8));
        assert_eq!(protobuf_varint(&[0x1a, 0x01, b'x'], 1), None);
        // 截断的字段不 panic
        assert_eq!(protobuf_varint(&[0x1a, 0x05, b'x'], 1), None);
    }
}
