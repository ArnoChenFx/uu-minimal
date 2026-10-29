//! 端口映射的被控端：Message 27 `PortMappingFrame` 落在 FILE_DATA_CHANNEL 上。
//!
//! 规则由 viewer 给出（viewer 的本地端口 → 本 host 可达的 target_host:target_port）。
//! viewer 每条本地连接开一条流，帧里带 (session_id, rule_id, stream_id) 三个标识，
//! host 负责连目标并把字节双向搬过去。帧类型：SYN=0、FIN=1、DATA=2、SYN_ACK=3、DATA_ACK=4；
//! SYN 载荷 `{"version","target_host","target_port"}`，SYN_ACK 载荷
//! `{"version","ok","error"}`；DATA 是裸字节；DATA_ACK 两侧都不看。载荷字节从不写日志。
//! `UU_PORT_MAPPING=0` 关闭（与官方被控端开关同名）。

use crate::proto::{read_fields, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use webrtc::data_channel::RTCDataChannel;

pub const MESSAGE_FIELD: u32 = 27;

const VERSION: u64 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_CHUNK: usize = 16 * 1024;
/// 一条流写往目标前允许排队的帧数，超了就按官方行为关流（"写缓冲超限"）。
const QUEUE_FRAMES: usize = 256;
/// data channel 上积压到这么多就暂停读目标（官方 host 的背压阈值）。
const BUFFER_HIGH_WATER: usize = 4 * 1024 * 1024;
/// 等 FILE_DATA_CHANNEL 打开的上限：100 × 50 ms。
const CHANNEL_WAIT: (u32, Duration) = (100, Duration::from_millis(50));

pub fn enabled() -> bool {
    std::env::var("UU_PORT_MAPPING").map_or(true, |value| value != "0")
}

/// protobuf 里的帧类型；未知值按官方实现忽略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Syn,
    Fin,
    Data,
    SynAck,
    DataAck,
}

impl Kind {
    fn from_wire(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::Syn),
            1 => Some(Self::Fin),
            2 => Some(Self::Data),
            3 => Some(Self::SynAck),
            4 => Some(Self::DataAck),
            _ => None,
        }
    }

    fn wire(self) -> u64 {
        match self {
            Self::Syn => 0,
            Self::Fin => 1,
            Self::Data => 2,
            Self::SynAck => 3,
            Self::DataAck => 4,
        }
    }
}

/// 三个标识字段必须按收到的类型原样回显：viewer 可能用 varint，也可能用字符串。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Ident {
    Int(u64),
    Text(Vec<u8>),
    Absent,
}

impl Ident {
    fn read(fields: &[crate::proto::Field], number: u32) -> Self {
        match fields.iter().find(|field| field.number == number).map(|field| field.value) {
            Some(Value::Varint(value)) => Self::Int(value),
            Some(Value::Bytes(bytes)) => Self::Text(bytes.to_vec()),
            _ => Self::Absent,
        }
    }

    fn write(&self, number: u32, out: &mut Vec<u8>) {
        match self {
            Self::Int(value) => {
                put_tag(out, number, 0);
                put_int(out, *value);
            }
            Self::Text(bytes) => put_blob(out, number, bytes),
            Self::Absent => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    session: Ident,
    rule: Ident,
    stream: Ident,
}

fn put_int(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

fn put_tag(out: &mut Vec<u8>, number: u32, wire_type: u64) {
    put_int(out, (u64::from(number) << 3) | wire_type);
}

fn put_blob(out: &mut Vec<u8>, number: u32, bytes: &[u8]) {
    put_tag(out, number, 2);
    put_int(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// 组帧：标识按原类型回显，类型 0 与空载荷按 proto3 默认值省略。
fn encode(key: &Key, kind: Kind, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 24);
    key.session.write(1, &mut out);
    key.rule.write(2, &mut out);
    key.stream.write(3, &mut out);
    if kind != Kind::Syn {
        put_tag(&mut out, 4, 0);
        put_int(&mut out, kind.wire());
    }
    if !payload.is_empty() {
        put_blob(&mut out, 5, payload);
    }
    out
}

/// SYN 载荷；官方是 snake_case，camelCase 也认（不同客户端版本出现过）。
fn target_of(payload: &[u8]) -> Result<(String, u16), String> {
    let json: serde_json::Value = serde_json::from_slice(payload).map_err(|_| "invalid SYN payload")?;
    let pick = |snake: &str, camel: &str| json.get(snake).or_else(|| json.get(camel)).cloned();
    let host = pick("target_host", "targetHost")
        .and_then(|value| value.as_str().map(str::trim).map(str::to_owned))
        .filter(|host| !host.is_empty())
        .ok_or("missing target host")?;
    let port = pick("target_port", "targetPort")
        .and_then(|value| value.as_u64())
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .ok_or("invalid target port")?;
    Ok((host, port))
}

fn answer_of(ok: bool, error: Option<&str>) -> Vec<u8> {
    let mut value = serde_json::json!({"version": VERSION, "ok": ok});
    if let Some(error) = error {
        value["error"] = serde_json::Value::from(error);
    }
    value.to_string().into_bytes()
}

/// 一条已建立的目标连接：写入队列 + 读目标的任务句柄。
struct Pipe {
    writes: mpsc::Sender<Vec<u8>>,
    reader: AbortHandle,
}

pub struct Session {
    /// 所有出向帧都排这一条队列，保证同一流的 SYN_ACK 先于 DATA。
    outbound: mpsc::Sender<(Key, Kind, Vec<u8>)>,
    pipes: Mutex<HashMap<Key, Pipe>>,
}

impl Session {
    /// `channel` 是 peer 的 FILE_DATA_CHANNEL 槽位，viewer 打开前是 `None`。
    pub fn new(channel: Arc<Mutex<Option<Arc<RTCDataChannel>>>>, sequence: Arc<AtomicU64>) -> Arc<Self> {
        let (outbound, mut queue) = mpsc::channel::<(Key, Kind, Vec<u8>)>(64);
        tokio::spawn(async move {
            while let Some((key, kind, payload)) = queue.recv().await {
                let Some(channel) = open_channel(&channel).await else {
                    log::warn!("Port mapping frame dropped: no FILE_DATA_CHANNEL");
                    continue;
                };
                while channel.buffered_amount().await > BUFFER_HIGH_WATER {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                let message = crate::proto::envelope(
                    sequence.fetch_add(1, Ordering::Relaxed),
                    timestamp,
                    MESSAGE_FIELD,
                    &encode(&key, kind, &payload),
                );
                if let Err(error) = channel.send(&bytes::Bytes::from(message)).await {
                    log::warn!("Port mapping send failed: {}", error);
                    break;
                }
            }
        });
        Arc::new(Self { outbound, pipes: Mutex::new(HashMap::new()) })
    }

    /// 处理 Message 27；返回 true 表示这条消息属于端口映射（无论是否接受）。
    pub fn handle(self: &Arc<Self>, message: &[crate::proto::Field], channel: &str) -> bool {
        let Some(raw) = message.iter().find(|field| field.number == MESSAGE_FIELD).and_then(|field| field.blob())
        else {
            return false;
        };
        if channel != "FILE_DATA_CHANNEL" {
            log::warn!("Port mapping frame ignored on {}", channel);
            return true;
        }
        let Ok(fields) = read_fields(raw) else {
            log::warn!("Port mapping frame undecodable");
            return true;
        };
        let key = Key {
            session: Ident::read(&fields, 1),
            rule: Ident::read(&fields, 2),
            stream: Ident::read(&fields, 3),
        };
        let kind = fields
            .iter()
            .find_map(|field| match (field.number, field.value) {
                (4, Value::Varint(value)) => Some(value),
                _ => None,
            })
            .map_or(Some(Kind::Syn), |wire| Kind::from_wire(wire));
        let payload = fields
            .iter()
            .find_map(|field| match field.value {
                Value::Bytes(bytes) if field.number == 5 => Some(bytes),
                _ => None,
            })
            .unwrap_or(&[]);
        match kind {
            Some(Kind::Syn) => self.accept_syn(key, payload),
            Some(Kind::Data) => self.forward(key, payload),
            Some(Kind::Fin) => self.close(&key, false),
            Some(Kind::DataAck) | Some(Kind::SynAck) => {}
            None => log::info!("Port mapping unsupported frame type"), // 未知类型：忽略
        }
        true
    }

    /// 把 viewer 的 DATA 写进目标；没有这条流就回 FIN 让 viewer 收尾。
    fn forward(self: &Arc<Self>, key: Key, payload: &[u8]) {
        let writes = self.pipes.lock().unwrap().get(&key).map(|pipe| pipe.writes.clone());
        match writes {
            Some(writes) => {
                if writes.try_send(payload.to_vec()).is_err() {
                    log::warn!("Port mapping write buffer full; closing stream");
                    self.close(&key, true);
                }
            }
            None => {
                let _ = self.outbound.try_send((key, Kind::Fin, Vec::new()));
            }
        }
    }

    fn accept_syn(self: &Arc<Self>, key: Key, payload: &[u8]) {
        let target = if enabled() { target_of(payload) } else { Err("port mapping disabled on this host".into()) };
        let (host, port) = match target {
            Ok(target) => target,
            Err(error) => {
                log::info!("Port mapping SYN rejected: {}", error);
                let _ = self.outbound.try_send((key, Kind::SynAck, answer_of(false, Some(&error))));
                return;
            }
        };
        let session = self.clone();
        tokio::spawn(async move {
            // 目标地址本身也算敏感，只记端口与是否回环。
            let loopback = host == "localhost" || host == "::1" || host.starts_with("127.");
            log::info!("Port mapping connecting: port={} loopback={}", port, loopback);
            let stream = match tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect((host.as_str(), port))).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return session.fail(key, format!("connect failed: {}", error.kind())).await,
                Err(_) => return session.fail(key, "connect timeout".to_owned()).await,
            };
            let _ = stream.set_nodelay(true);
            let (mut source, mut sink) = stream.into_split();
            let (writes, mut queued) = mpsc::channel::<Vec<u8>>(QUEUE_FRAMES);
            // 同一队列保证 SYN_ACK 排在第一条 DATA 前面。
            if session.outbound.send((key.clone(), Kind::SynAck, answer_of(true, None))).await.is_err() {
                return;
            }
            log::info!("Port mapping stream open: port={}", port);
            tokio::spawn(async move {
                while let Some(chunk) = queued.recv().await {
                    if sink.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
                let _ = sink.shutdown().await;
            });
            let (ready, wait_ready) = tokio::sync::oneshot::channel::<()>();
            let owner = session.clone();
            let key_for_reader = key.clone();
            let reader = tokio::spawn(async move {
                // 注册完成后再开始读，早到的 EOF 才不会丢。
                if wait_ready.await.is_err() {
                    return;
                }
                let mut buffer = vec![0u8; READ_CHUNK];
                loop {
                    match source.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => {
                            if owner.outbound.send((key_for_reader.clone(), Kind::Data, buffer[..read].to_vec())).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                owner.close(&key_for_reader, true);
            });
            session.pipes.lock().unwrap().insert(key, Pipe { writes, reader: reader.abort_handle() });
            let _ = ready.send(());
        });
    }

    async fn fail(&self, key: Key, error: String) {
        log::info!("Port mapping connect failed: {}", error);
        let _ = self.outbound.send((key, Kind::SynAck, answer_of(false, Some(&error)))).await;
    }

    /// 丢掉一条流：写任务会因为队列关闭而 shutdown 目标，读任务直接 abort；
    /// `tell_peer` 时补一条 FIN。
    fn close(&self, key: &Key, tell_peer: bool) {
        let Some(pipe) = self.pipes.lock().unwrap().remove(key) else { return };
        pipe.reader.abort();
        if tell_peer {
            let _ = self.outbound.try_send((key.clone(), Kind::Fin, Vec::new()));
        }
    }

    /// peer connection 结束：所有目标连接一起关。
    pub fn on_peer_state(&self, state: webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState) {
        use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
        // Disconnected 仍可由 ICE 自动恢复；保留 TCP 流直到明确失败或关闭。
        if matches!(state, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
            self.shutdown();
        }
    }

    pub fn shutdown(&self) {
        let pipes: Vec<Pipe> = self.pipes.lock().unwrap().drain().map(|(_, pipe)| pipe).collect();
        if !pipes.is_empty() {
            log::info!("Port mapping closed {} stream(s) with the connection", pipes.len());
        }
        for pipe in pipes {
            pipe.reader.abort();
        }
    }
}

/// 等 FILE_DATA_CHANNEL 打开，最多 `CHANNEL_WAIT`；拿不到返回 None（这一帧丢弃）。
async fn open_channel(slot: &Arc<Mutex<Option<Arc<RTCDataChannel>>>>) -> Option<Arc<RTCDataChannel>> {
    for attempt in 0..=CHANNEL_WAIT.0 {
        if let Some(channel) = slot.lock().unwrap().clone() {
            return Some(channel);
        }
        if attempt == CHANNEL_WAIT.0 {
            break;
        }
        tokio::time::sleep(CHANNEL_WAIT.1).await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Field;

    #[tokio::test]
    async fn transient_disconnect_preserves_pipes_but_failure_closes_them() {
        use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState as State;
        for terminal in [State::Failed, State::Closed] {
            let session = Session::new(Arc::new(Mutex::new(None)), Arc::new(AtomicU64::new(0)));
            let key = Key { session: Ident::Int(1), rule: Ident::Int(2), stream: Ident::Int(3) };
            let (writes, mut rx) = mpsc::channel(1);
            let reader = tokio::spawn(std::future::pending::<()>());
            session.pipes.lock().unwrap().insert(key.clone(), Pipe { writes, reader: reader.abort_handle() });
            session.on_peer_state(State::Disconnected);
            session.on_peer_state(State::Connected);
            session.forward(key, b"after recovery");
            assert_eq!(rx.recv().await.unwrap(), b"after recovery");
            assert!(!reader.is_finished());
            session.on_peer_state(terminal);
            assert!(rx.recv().await.is_none());
            assert!(reader.await.unwrap_err().is_cancelled());
        }
    }

    #[test]
    fn syn_payload_accepts_both_key_styles() {
        assert_eq!(target_of(br#"{"version":1,"target_host":"127.0.0.1","target_port":22}"#).unwrap(), ("127.0.0.1".into(), 22));
        assert_eq!(target_of(br#"{"targetHost":"nas.local","targetPort":8080}"#).unwrap(), ("nas.local".into(), 8080));
        assert!(target_of(br#"{"target_host":"","target_port":22}"#).is_err());
        assert!(target_of(br#"{"target_host":"h","target_port":70000}"#).is_err());
        assert!(target_of(b"not json").is_err());
    }

    #[test]
    fn syn_ack_is_json_with_version_and_error() {
        let ok: serde_json::Value = serde_json::from_slice(&answer_of(true, None)).unwrap();
        assert_eq!(ok, serde_json::json!({"version": 1, "ok": true}));
        let failed: serde_json::Value = serde_json::from_slice(&answer_of(false, Some("connect timeout"))).unwrap();
        assert_eq!(failed["ok"], false);
        assert_eq!(failed["error"], "connect timeout");
    }

    #[test]
    fn identity_fields_keep_their_wire_type() {
        let key = Key { session: Ident::Text(b"s1".to_vec()), rule: Ident::Int(7), stream: Ident::Int(300) };
        let packet = encode(&key, Kind::Data, b"abc");
        let fields = read_fields(&packet).unwrap();
        assert_eq!(Ident::read(&fields, 1), Ident::Text(b"s1".to_vec()));
        assert_eq!(Ident::read(&fields, 2), Ident::Int(7));
        assert_eq!(Ident::read(&fields, 3), Ident::Int(300));
        assert!(fields.iter().any(|field| field.number == 4 && field.value == Value::Varint(2)));
        assert!(fields.iter().any(|field| field.number == 5 && field.value == Value::Bytes(b"abc")));
        // SYN 与空载荷是 proto3 默认值，不上线。
        let empty = encode(&key, Kind::Syn, b"");
        let syn = read_fields(&empty).unwrap();
        assert!(!syn.iter().any(|field| field.number == 4 || field.number == 5));
    }

    #[test]
    fn frame_kind_tables_round_trip_and_reject_unknown() {
        for kind in [Kind::Syn, Kind::Fin, Kind::Data, Kind::SynAck, Kind::DataAck] {
            assert_eq!(Kind::from_wire(kind.wire()), Some(kind));
        }
        assert_eq!(Kind::from_wire(5), None);
    }

    #[tokio::test]
    async fn handle_ignores_frames_outside_file_channel() {
        let session = Session::new(Arc::new(Mutex::new(None)), Arc::new(AtomicU64::new(0)));
        let frame = encode(&Key { session: Ident::Int(1), rule: Ident::Int(1), stream: Ident::Int(1) }, Kind::Syn, b"{}");
        let message = vec![Field { number: MESSAGE_FIELD, value: Value::Bytes(&frame) }];
        assert!(session.handle(&message, "BINARY_DATA_CHANNEL"));
        // 不是 Message 27 的消息不归端口映射管。
        let other = vec![Field { number: 22, value: Value::Bytes(b"") }];
        assert!(!session.handle(&other, "FILE_DATA_CHANNEL"));
    }
}
