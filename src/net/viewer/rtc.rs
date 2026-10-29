//! viewer 侧 PeerConnection：发 offer、收 answer/candidate、开五条通道，
//! 并在 FILE 通道上跑 `Message 27` 的多路流。

use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify};
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_credential_type::RTCIceCredentialType;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use super::signal::Signal;
use crate::proto::{read_fields, Value as WireValue};

pub const CHANNELS: [&str; 5] =
    ["CONTROL_DATA_CHANNEL", "TEXT_DATA_CHANNEL", "STREAMER_DATA_CHANNEL", "FILE_DATA_CHANNEL", "BINARY_DATA_CHANNEL"];
pub const FILE_CHANNEL: usize = 3;
const MESSAGE_FIELD: u32 = 27;
const SYN: u64 = 0;
const FIN: u64 = 1;
const DATA: u64 = 2;
const SYN_ACK: u64 = 3;
const SESSION_ID: u64 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// ack 里的 iceServers / force_relay → RTCConfiguration。
/// 云端没给就用 host 候选，**不自造公共 STUN**。
pub fn configuration(ack: &Value) -> Result<RTCConfiguration, String> {
    let list = ack.get("iceServers").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut servers = Vec::new();
    for server in &list {
        let urls: Vec<String> = match server.get("urls") {
            Some(Value::String(url)) => vec![url.clone()],
            Some(Value::Array(urls)) => urls.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
            _ => Vec::new(),
        };
        let urls: Vec<String> = urls
            .into_iter()
            .filter(|url| url.len() <= 2048 && ["stun:", "stuns:", "turn:", "turns:"].iter().any(|p| url.starts_with(p)))
            .collect();
        if urls.is_empty() {
            continue;
        }
        let string = |key: &str| server.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
        servers.push(RTCIceServer {
            urls,
            username: string("username"),
            credential: string("credential"),
            credential_type: RTCIceCredentialType::Password,
        });
    }
    let relay = ack.get("force_relay").and_then(Value::as_bool).unwrap_or(false);
    if relay && !servers.iter().any(|s| s.urls.iter().any(|u| u.starts_with("turn:") || u.starts_with("turns:"))) {
        return Err("云端要求 relay-only，但没有 TURN".into());
    }
    Ok(RTCConfiguration {
        ice_servers: servers,
        ice_transport_policy: if relay { RTCIceTransportPolicy::Relay } else { RTCIceTransportPolicy::All },
        ..Default::default()
    })
}

struct Flow {
    data: mpsc::Sender<Vec<u8>>,
    ack: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

pub struct Peer {
    pub pc: Arc<RTCPeerConnection>,
    pub channels: Vec<Arc<RTCDataChannel>>,
    app_id: String,
    ice_id: String,
    remote: String,
    answered: AtomicBool,
    early: Mutex<Vec<RTCIceCandidateInit>>,
    flows: Arc<Mutex<HashMap<(u64, u64), Arc<Flow>>>>,
    next_rule: AtomicU64,
    signal: Arc<Signal>,
    transport_changed: Arc<Notify>,
}

impl Peer {
    pub async fn new(config: RTCConfiguration, app_id: String, ice_id: String, remote: String, signal: Arc<Signal>) -> Result<Self, String> {
        let api = APIBuilder::new().with_media_engine(MediaEngine::default()).build();
        let pc = Arc::new(api.new_peer_connection(config).await.map_err(|e| format!("创建 PeerConnection 失败: {e}"))?);
        let transport_changed = Arc::new(Notify::new());
        let changed = transport_changed.clone();
        pc.on_peer_connection_state_change(Box::new(move |_| {
            let changed = changed.clone();
            Box::pin(async move { changed.notify_one(); })
        }));

        let (app, ice, client) = (app_id.clone(), ice_id.clone(), remote.clone());
        let weak = Arc::downgrade(&pc);
        let signal_for_candidates = signal.clone();
        pc.on_ice_candidate(Box::new(move |candidate: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let (weak, signal, app, ice, client) = (weak.clone(), signal_for_candidates.clone(), app.clone(), ice.clone(), client.clone());
            Box::pin(async move {
                let Some(candidate) = candidate else { return };
                let Ok(mut value) = candidate.to_json() else { return };
                // 回调可能不带 MID：绑定到 answer/offer 的第一个 bundled m-line。
                if let Some(pc) = weak.upgrade() {
                    if let Some(local) = pc.local_description().await {
                        if let Some(mid) = local.sdp.lines().find_map(|line| line.strip_prefix("a=mid:")) {
                            value.sdp_mid = Some(mid.trim().to_owned());
                            value.sdp_mline_index = Some(0);
                        }
                    }
                }
                let payload = json!({"client_id": client, "data": {
                    "type": "candidate", "candidate": value, "ice_id": ice, "app_control_id": app}});
                if let Err(e) = signal.soac(payload).await {
                    log::warn!("发送候选失败: {e}");
                }
            })
        }));

        let flows: Arc<Mutex<HashMap<(u64, u64), Arc<Flow>>>> = Arc::new(Mutex::new(HashMap::new()));
        {
            let flows = flows.clone();
            pc.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
                if channel.label() == CHANNELS[FILE_CHANNEL] {
                    let flows = flows.clone();
                    channel.on_message(Box::new(move |message: DataChannelMessage| {
                        let flows = flows.clone();
                        Box::pin(async move { dispatch_flow(&flows, &message.data).await })
                    }));
                }
                Box::pin(async {})
            }));
        }

        let mut channels = Vec::with_capacity(CHANNELS.len());
        for label in CHANNELS {
            let channel = pc
                .create_data_channel(label, Some(RTCDataChannelInit { ordered: Some(true), ..Default::default() }))
                .await
                .map_err(|e| format!("创建 data channel {label} 失败: {e}"))?;
            channels.push(channel);
        }

        {
            // `on_data_channel` 只在远端建通道时触发；自己 create 的通道必须自己挂 on_message，
            // 否则 FILE 通道上的 Message 27（SYN_ACK/DATA）全部收不到。
            let flows = flows.clone();
            channels[FILE_CHANNEL].on_message(Box::new(move |message: DataChannelMessage| {
                let flows = flows.clone();
                Box::pin(async move { dispatch_flow(&flows, &message.data).await })
            }));
            let changed = transport_changed.clone();
            channels[FILE_CHANNEL].on_close(Box::new(move || {
                let changed = changed.clone();
                Box::pin(async move { changed.notify_one(); })
            }));
        }

        Ok(Self {
            pc,
            channels,
            app_id,
            ice_id,
            remote,
            answered: AtomicBool::new(false),
            early: Mutex::new(Vec::new()),
            flows,
            next_rule: AtomicU64::new(1),
            signal,
            transport_changed,
        })
    }

    /// offer 必须在 host 处理完 `be-controlled` 之后才发（官方 host 会丢弃未知 ice_id 的 offer）。
    pub async fn offer(&self) -> Result<Value, String> {
        let offer = self.pc.create_offer(None).await.map_err(|e| format!("生成 offer 失败: {e}"))?;
        let mut sdp = offer.sdp.clone();
        self.pc.set_local_description(offer).await.map_err(|e| format!("设置本地 SDP 失败: {e}"))?;
        if !sdp.contains("a=x-uuremote-quic") {
            if let Some(index) = sdp.find("a=group:BUNDLE") {
                sdp.insert_str(index, "a=x-uuremote-quic\r\n");
            }
        }
        Ok(json!({"client_id": self.remote, "data": {
            "type": "offer",
            "sdp": crate::net::peer::with_max_message_size(&sdp),
            "ice_id": self.ice_id,
            "app_control_id": self.app_id,
            "ice_network_type": 3,
        }}))
    }

    pub async fn receive(&self, event: &Value) -> Result<(), String> {
        let body = event.get("data").unwrap_or(event);
        if let Some(ice_id) = body.get("ice_id").and_then(Value::as_str) {
            if ice_id != self.ice_id {
                return Ok(());
            }
        }
        if let Some(app) = body.get("app_control_id").and_then(Value::as_str) {
            if app != self.app_id {
                return Ok(());
            }
        }
        match body.get("type").and_then(Value::as_str) {
            Some("answer") => {
                if self.answered.swap(true, Ordering::AcqRel) {
                    return Ok(());
                }
                let sdp = answer_sdp(body)?;
                let answer = RTCSessionDescription::answer(sdp).map_err(|e| format!("answer 无效: {e}"))?;
                self.pc.set_remote_description(answer).await.map_err(|e| format!("设置远端 SDP 失败: {e}"))?;
                // 先把队列取空再 await：MutexGuard 跨 await 会让 future 不是 Send。
                let early: Vec<RTCIceCandidateInit> = self.early.lock().unwrap().drain(..).collect();
                for candidate in early {
                    self.pc.add_ice_candidate(candidate).await.map_err(|e| format!("回放候选失败: {e}"))?;
                }
            }
            Some("candidate") => {
                let Some(value) = body.get("candidate") else { return Ok(()) };
                let candidate: RTCIceCandidateInit = if let Some(text) = value.as_str() {
                    RTCIceCandidateInit {
                        candidate: text.to_owned(),
                        sdp_mid: body.get("sdp_mid").and_then(Value::as_str).map(str::to_owned),
                        sdp_mline_index: body.get("sdp_mline_index").and_then(Value::as_u64).and_then(|v| u16::try_from(v).ok()),
                        ..Default::default()
                    }
                } else {
                    serde_json::from_value(value.clone()).map_err(|e| format!("候选无效: {e}"))?
                };
                if self.answered.load(Ordering::Acquire) {
                    self.pc.add_ice_candidate(candidate).await.map_err(|e| format!("添加候选失败: {e}"))?;
                } else {
                    let mut early = self.early.lock().unwrap();
                    if early.len() >= 256 {
                        return Err("过早的候选过多".into());
                    }
                    early.push(candidate);
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn wait_channel(&self, index: usize, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.channels[index].ready_state() == RTCDataChannelState::Open {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("{} 未在 {:?} 内打开（对端可能没接受这次连接）", CHANNELS[index], timeout));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn send_file(&self, message: Vec<u8>) -> Result<(), String> {
        tokio::time::timeout(CONNECT_TIMEOUT, self.channels[FILE_CHANNEL].send(&Bytes::from(message)))
            .await.map_err(|_| "FILE 通道发送超时".to_string())?
            .map(|_| ())
            .map_err(|e| format!("FILE 通道发送失败: {e}"))
    }

    /// 打开一条流：SYN → 等 SYN_ACK，之后双向 DATA。
    pub async fn open_flow(self: &Arc<Self>, host: &str, port: u16) -> Result<FlowHandle, String> {
        let rule = self.next_rule.fetch_add(1, Ordering::Relaxed);
        let stream = 1u64;
        let (data, data_rx) = mpsc::channel::<Vec<u8>>(256);
        let (ack, ack_rx) = oneshot::channel();
        let flow = Arc::new(Flow { data, ack: Mutex::new(Some(ack)) });
        self.flows.lock().unwrap().insert((rule, stream), flow);

        let syn = json!({"version": 1, "target_host": host, "target_port": port}).to_string();
        self.send_file(frame(rule, stream, SYN, syn.as_bytes())).await?;

        match tokio::time::timeout(CONNECT_TIMEOUT, ack_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => {
                self.flows.lock().unwrap().remove(&(rule, stream));
                return Err(e);
            }
            _ => {
                self.flows.lock().unwrap().remove(&(rule, stream));
                return Err("等待 SYN_ACK 超时".into());
            }
        }
        Ok(FlowHandle { peer: self.clone(), rule, stream, rx: data_rx })
    }

    /// 保活探测流：走 FILE 通道开一条真实流（SYN → SYN_ACK → FIN），让
    /// TURN 中继租约与 NAT 映射一直有流量。云端的 Message 27 语义下这
    /// 与普通连接无异；host 连一个不存在端口会回 SYN_ACK error，同样
    /// 能证明链路活着——只要 SYN 发出去且有回包，SCTP/DTLS/ICE 全链路
    /// 都有流量。返回是否收到回包（目标端口拒绝也算链路正常）。
    pub async fn keepalive_ping(&self) -> bool {
        let rule = self.next_rule.fetch_add(1, Ordering::Relaxed);
        let stream = 1u64;
        let (data, _data_rx) = mpsc::channel::<Vec<u8>>(16);
        let (ack, ack_rx) = oneshot::channel();
        let flow = Arc::new(Flow { data, ack: Mutex::new(Some(ack)) });
        self.flows.lock().unwrap().insert((rule, stream), flow);

        let syn = json!({"version": 1, "target_host": "127.0.0.1", "target_port": 1}).to_string();
        if let Err(e) = self.send_file(frame(rule, stream, SYN, syn.as_bytes())).await {
            log::debug!("keepalive 发送失败（链路可能已断）: {e}");
            self.flows.lock().unwrap().remove(&(rule, stream));
            return false;
        }
        let alive = matches!(tokio::time::timeout(CONNECT_TIMEOUT, ack_rx).await, Ok(Ok(_)));
        log::debug!("keepalive: reply_received={alive}");
        // 无论结果如何都清掉这条流；host 侧若真连上了 127.0.0.1:1 也会因
        // 收到我们的 FIN 立即释放。
        self.flows.lock().unwrap().remove(&(rule, stream));
        let _ = self.send_file(frame(rule, stream, FIN, &[])).await;
        alive
    }

    pub async fn close(&self) {
        self.signal.shutdown();
        self.flows.lock().unwrap().clear();
        let _ = tokio::time::timeout(CONNECT_TIMEOUT, self.pc.close()).await;
    }

    pub fn transport_closed(&self) -> bool {
        use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
        matches!(self.pc.connection_state(), RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed)
            || self.channels[FILE_CHANNEL].ready_state() != RTCDataChannelState::Open
    }

    pub async fn wait_transport_closed(&self) {
        loop {
            let changed = self.transport_changed.notified();
            if self.transport_closed() { return; }
            changed.await;
        }
    }

    /// 硬断线时中断正在等待回包的探测；调用方随后 close 清理其待处理流。
    pub async fn probe_or_closed(&self) -> Option<bool> {
        tokio::select! {
            biased;
            _ = self.wait_transport_closed() => None,
            alive = self.keepalive_ping() => Some(alive),
        }
    }
}

pub struct FlowHandle {
    peer: Arc<Peer>,
    rule: u64,
    stream: u64,
    rx: mpsc::Receiver<Vec<u8>>,
}

/// 读写两半：双向泵可以同时持有。
pub struct FlowWriter {
    peer: Arc<Peer>,
    rule: u64,
    stream: u64,
}

pub struct FlowReader {
    rx: mpsc::Receiver<Vec<u8>>,
}

impl FlowHandle {
    pub fn into_parts(self) -> (FlowWriter, FlowReader) {
        (
            FlowWriter { peer: self.peer, rule: self.rule, stream: self.stream },
            FlowReader { rx: self.rx },
        )
    }
}

impl FlowWriter {
    pub async fn send(&self, data: &[u8]) -> Result<(), String> {
        for chunk in data.chunks(16 * 1024) {
            self.peer.send_file(frame(self.rule, self.stream, DATA, chunk)).await?;
        }
        Ok(())
    }

    pub async fn fin(&self) -> Result<(), String> {
        self.peer.send_file(frame(self.rule, self.stream, FIN, &[])).await
    }
}

impl FlowReader {
    /// None = 对端 FIN 或连接断开。
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.rx.recv().await
    }
}

async fn dispatch_flow(flows: &Arc<Mutex<HashMap<(u64, u64), Arc<Flow>>>>, data: &[u8]) {
    let Ok(fields) = read_fields(data) else { return };
    let Some(WireValue::Bytes(raw)) = fields.iter().find(|f| f.number == MESSAGE_FIELD).map(|f| &f.value) else { return };
    let Ok(frame) = read_fields(raw) else { return };
    let number = |n: u32| frame.iter().find(|f| f.number == n).and_then(|f| match f.value { WireValue::Varint(v) => Some(v), _ => None });
    let (rule, stream) = (number(2).unwrap_or(0), number(3).unwrap_or(0));
    let kind = number(4).unwrap_or(0);
    let payload = frame
        .iter()
        .find(|f| f.number == 5)
        .and_then(|f| match f.value { WireValue::Bytes(b) => Some(b.to_vec()), _ => None })
        .unwrap_or_default();
    let flow = flows.lock().unwrap().get(&(rule, stream)).cloned();
    let Some(flow) = flow else {
        log::warn!("端口映射帧没有对应的流，忽略");
        return;
    };
    match kind {
        DATA => {
            // TCP 隧道必须逐字节可靠：接收慢时施加背压，不能静默丢帧。
            let _ = flow.data.send(payload).await;
        }
        FIN => {
            flows.lock().unwrap().remove(&(rule, stream));
        }
        SYN_ACK => {
            if let Some(tx) = flow.ack.lock().unwrap().take() {
                let reply = serde_json::from_slice::<Value>(&payload).ok();
                let ok = reply.as_ref().and_then(|v| v.get("ok").and_then(Value::as_bool)).unwrap_or(false);
                // 对端的 error 是 host 侧连不上的原因（refused / timeout），不是凭据。
                let error = reply
                    .as_ref()
                    .and_then(|v| v.get("error").and_then(Value::as_str))
                    .unwrap_or("目标不可达")
                    .to_owned();
                let _ = tx.send(if ok { Ok(()) } else { Err(format!("目标不可达: {error}")) });
            }
        }
        _ => {}
    }
}

fn frame(rule: u64, stream: u64, kind: u64, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    int(1, SESSION_ID, &mut body);
    int(2, rule, &mut body);
    int(3, stream, &mut body);
    if kind != SYN {
        int(4, kind, &mut body);
    }
    if !payload.is_empty() {
        bytes(5, payload, &mut body);
    }
    crate::proto::envelope(0, 0, MESSAGE_FIELD, &body)
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn int(field: u32, value: u64, out: &mut Vec<u8>) {
    varint(u64::from(field) << 3, out);
    varint(value, out);
}

fn bytes(field: u32, value: &[u8], out: &mut Vec<u8>) {
    varint((u64::from(field) << 3) | 2, out);
    varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

/// 官方 host 可能回 gzip_sdp（base64 + gzip）。
fn answer_sdp(body: &Value) -> Result<String, String> {
    if let Some(sdp) = body.get("sdp").and_then(Value::as_str) {
        if sdp.len() <= 1024 * 1024 {
            return Ok(sdp.to_owned());
        }
    }
    if let Some(encoded) = body.get("gzip_sdp").and_then(Value::as_str) {
        use base64::Engine;
        use std::io::Read;
        if encoded.len() > 2 * 1024 * 1024 {
            return Err("压缩 answer 过大".into());
        }
        let raw = base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_| "压缩 answer 无效")?;
        let mut sdp = String::new();
        flate2::read::GzDecoder::new(raw.as_slice())
            .take(1024 * 1024 + 1)
            .read_to_string(&mut sdp)
            .map_err(|_| "压缩 answer 解压失败")?;
        if sdp.len() <= 1024 * 1024 {
            return Ok(sdp);
        }
    }
    Err("answer 缺少 sdp".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_close_interrupts_an_inflight_probe_on_a_live_peer_connection() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.send(Message::Text("0{}".into())).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "40");
            socket.send(Message::Text("40{}".into())).await.unwrap();
            while let Some(Ok(_)) = socket.next().await {}
        });
        let (events, _rx) = mpsc::channel(8);
        let signal = Signal::connect(&format!("ws://{address}"), "test", Duration::from_secs(2), events).await.unwrap();
        let peer = Peer::new(RTCConfiguration::default(), "app".into(), "ice".into(), "remote".into(), signal).await.unwrap();
        let remote = APIBuilder::new().build().new_peer_connection(RTCConfiguration::default()).await.unwrap();
        let (file_tx, file_rx) = oneshot::channel();
        let file_tx = Arc::new(Mutex::new(Some(file_tx)));
        remote.on_data_channel(Box::new(move |channel| {
            if channel.label() == CHANNELS[FILE_CHANNEL] {
                if let Some(tx) = file_tx.lock().unwrap().take() { let _ = tx.send(channel); }
            }
            Box::pin(async {})
        }));
        let offer = peer.pc.create_offer(None).await.unwrap();
        let mut gathered = peer.pc.gathering_complete_promise().await;
        peer.pc.set_local_description(offer).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), gathered.recv()).await.unwrap();
        remote.set_remote_description(peer.pc.local_description().await.unwrap()).await.unwrap();
        let answer = remote.create_answer(None).await.unwrap();
        let mut gathered = remote.gathering_complete_promise().await;
        remote.set_local_description(answer).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), gathered.recv()).await.unwrap();
        peer.pc.set_remote_description(remote.local_description().await.unwrap()).await.unwrap();
        peer.wait_channel(FILE_CHANNEL, Duration::from_secs(10)).await.unwrap();
        let remote_file = tokio::time::timeout(Duration::from_secs(2), file_rx).await.unwrap().unwrap();

        // 远端 FILE 通道不回复 SYN，探测本应等到 CONNECT_TIMEOUT。
        let probe = peer.probe_or_closed();
        tokio::pin!(probe);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut probe).await.is_err());
        remote_file.close().await.unwrap();
        assert_eq!(tokio::time::timeout(Duration::from_secs(2), &mut probe).await.unwrap(), None);
        assert_eq!(peer.pc.connection_state(), webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected);
        peer.close().await;
        remote.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closing_stale_peer_releases_waiters_and_signaling() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.send(Message::Text("0{}".into())).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap().into_text().unwrap(), "40");
            socket.send(Message::Text("40{}".into())).await.unwrap();
            // 保持信令连接，直到控制端显式释放旧会话。
            let ended = tokio::time::timeout(Duration::from_secs(3), socket.next()).await.unwrap();
            assert!(!matches!(ended, Some(Ok(Message::Text(_)))));
        });
        let (events, _rx) = mpsc::channel(8);
        let signal = Signal::connect(&format!("ws://{address}"), "test", Duration::from_secs(2), events).await.unwrap();
        let peer = Peer::new(RTCConfiguration::default(), "app".into(), "ice".into(), "remote".into(), signal).await.unwrap();
        let (data, mut rx) = mpsc::channel(1);
        let (ack, ack_rx) = oneshot::channel();
        peer.flows.lock().unwrap().insert((1, 1), Arc::new(Flow { data, ack: Mutex::new(Some(ack)) }));
        peer.close().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            assert!(rx.recv().await.is_none());
            assert!(ack_rx.await.is_err());
        }).await.unwrap();
        assert!(peer.transport_closed());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn full_receive_queue_applies_backpressure_without_losing_bytes() {
        let flows = Arc::new(Mutex::new(HashMap::new()));
        let (data, mut rx) = mpsc::channel(1);
        flows.lock().unwrap().insert((7, 1), Arc::new(Flow { data, ack: Mutex::new(None) }));
        dispatch_flow(&flows, &frame(7, 1, DATA, b"first")).await;
        let message = frame(7, 1, DATA, b"second");
        let pending = dispatch_flow(&flows, &message);
        tokio::pin!(pending);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut pending).await.is_err());
        assert_eq!(rx.recv().await.unwrap(), b"first");
        pending.await;
        assert_eq!(rx.recv().await.unwrap(), b"second");
        dispatch_flow(&flows, &frame(7, 1, FIN, &[])).await;
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn port_mapping_frames_round_trip() {
        let message = frame(7, 1, DATA, b"hello");
        let fields = read_fields(&message).unwrap();
        assert!(fields.iter().any(|f| f.number == MESSAGE_FIELD));
        let WireValue::Bytes(body) = fields.iter().find(|f| f.number == MESSAGE_FIELD).unwrap().value else { panic!() };
        let inner = read_fields(body).unwrap();
        let number = |n: u32| inner.iter().find(|f| f.number == n).and_then(|f| match f.value { WireValue::Varint(v) => Some(v), _ => None });
        assert_eq!((number(1), number(2), number(3), number(4)), (Some(1), Some(7), Some(1), Some(DATA)));
    }

    #[test]
    fn ice_configuration_rejects_relay_without_turn() {
        assert!(configuration(&json!({"force_relay": true})).is_err());
        let ok = configuration(&json!({"iceServers": [{"urls": "stun:127.0.0.1:3478"}]})).unwrap();
        assert_eq!(ok.ice_servers.len(), 1);
        assert_eq!(configuration(&json!({})).unwrap().ice_servers.len(), 0);
    }
}
