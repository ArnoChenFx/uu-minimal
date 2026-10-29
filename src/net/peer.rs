//! 组网的数据面：收 offer → 回 answer，并把本地 ICE 候选逐个发回。
//! host 永远是 DTLS Server（a=setup:passive），和官方 host 一致。
//! 这里只建连接，不采集画面、不注入输入——那是主程序的事。

use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::dtls_transport::dtls_role::DTLSRole;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::host::portmap;
use crate::net::ice::SessionIce;
use crate::proto::{self, Value as WireValue};
use crate::net::signaling::{Signaling, Soac};
use crate::host::terminal::{self, TerminalConn};

const FILE_DATA_CHANNEL: &str = "FILE_DATA_CHANNEL";
const BINARY_DATA_CHANNEL: &str = "BINARY_DATA_CHANNEL";

/// viewer（libwebrtc）默认按 64 KB 处理 data channel 单条消息；webrtc-rs 不写
/// a=max-message-size，端口映射的大帧会被对端拒绝，所以只对发给对端的那份 SDP 补一行。
const MAX_MESSAGE_SIZE: usize = 262_144;

/// 跳过 overlay / 容器网卡：否则同一 LAN 也会选到 ZeroTier 之类的候选（实测 36-81ms）。
fn interface_allowed(name: &str) -> bool {
    !["zt", "docker", "br-", "veth", "virbr", "lo"].iter().any(|prefix| name.starts_with(prefix))
}

/// 处理一条 offer：建 PeerConnection、回 answer，后续候选由回调自己发。
pub async fn answer(
    ice: &SessionIce,
    offer_sdp: &str,
    signaling: Arc<Signaling>,
    client_id: String,
    ice_id: String,
    app_control_id: Option<Value>,
) -> Result<Arc<RTCPeerConnection>, String> {
    if offer_sdp.is_empty() {
        return Err("offer SDP 为空".into());
    }

    let mut media = MediaEngine::default();
    media.register_default_codecs().map_err(|e| format!("注册默认编解码失败: {e}"))?;

    let mut settings = SettingEngine::default();
    settings
        .set_answering_dtls_role(DTLSRole::Server)
        .map_err(|e| format!("设置 DTLS 角色失败: {e}"))?;
    // mDNS 关掉：直接给真实网卡候选，和官方 host 一样。
    settings.set_ice_multicast_dns_mode(webrtc::ice::mdns::MulticastDnsMode::Disabled);
    settings.set_interface_filter(Box::new(|name: &str| interface_allowed(name)));

    let api = APIBuilder::new()
        .with_media_engine(media)
        .with_setting_engine(settings)
        .build();
    let pc = Arc::new(
        api.new_peer_connection(ice.configuration()?)
            .await
            .map_err(|e| format!("创建 PeerConnection 失败: {e}"))?,
    );

    // 候选要带 sdpMid / sdpMLineIndex；webrtc-rs 的 to_json() 固定给空 mid，
    // 所以从 answer 的 BUNDLE 首段自己算，收集开始前先填好。
    let bundle: Arc<Mutex<Option<(String, u16)>>> = Arc::new(Mutex::new(None));
    {
        let (signal, client, ice, control, slot) = (
            signaling.clone(),
            client_id.clone(),
            ice_id.clone(),
            app_control_id.clone(),
            bundle.clone(),
        );
        pc.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
            let (signal, client, ice, control, slot) =
                (signal.clone(), client.clone(), ice.clone(), control.clone(), slot.clone());
            Box::pin(async move {
                let Some(candidate) = candidate else {
                    log::info!("本地 ICE 候选收集完成");
                    return;
                };
                let Ok(init) = candidate.to_json() else { return };
                let Some((mid, mline)) = slot.lock().await.clone() else {
                    log::warn!("候选早于 BUNDLE 协商，丢弃");
                    return;
                };
                let soac = Soac {
                    kind: "candidate".into(),
                    candidate: Some(json!({
                        "candidate": init.candidate,
                        "sdpMid": mid,
                        "sdpMLineIndex": mline,
                    })),
                    ice_id: Some(Value::String(ice)),
                    client_id: (!client.is_empty()).then_some(client),
                    app_control_id: control,
                    ..Default::default()
                };
                if let Err(e) = signal.emit_soac(&soac).await {
                    log::error!("发送本地候选失败: {e}");
                }
            })
        }));
    }

    // ---- data channel 分发：终端（裸 TERM 帧 + Message 21 RPC）与端口映射（Message 27）----
    // 每个 peer 一个出口队列；发送时按通道名找已打开的 data channel。
    let channels: Arc<StdMutex<HashMap<String, Arc<RTCDataChannel>>>> = Arc::new(StdMutex::new(HashMap::new()));
    let file_channel: Arc<StdMutex<Option<Arc<RTCDataChannel>>>> = Arc::new(StdMutex::new(None));
    let sequence = Arc::new(AtomicU64::new(1));

    let (outbox, outbox_rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
    tokio::spawn(drain_outbox(outbox_rx, channels.clone()));

    // 终端输出的是裸 TERM 帧，这里补上通道名并入同一个出口。
    let (binary_tx, mut binary_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    {
        let outbox = outbox.clone();
        tokio::spawn(async move {
            while let Some(frame) = binary_rx.recv().await {
                if outbox.send((BINARY_DATA_CHANNEL.to_owned(), frame)).is_err() {
                    return;
                }
            }
        });
    }
    let terminal = Arc::new(TerminalConn::new(binary_tx));
    // 端口映射的帧固定走 FILE_DATA_CHANNEL，Session 自己持有那个槽位。
    let mapping = portmap::Session::new(file_channel.clone(), sequence.clone());

    {
        let mapping = mapping.clone();
        pc.on_peer_connection_state_change(Box::new(move |state: RTCPeerConnectionState| {
            log::info!("PeerConnection 状态: {state:?}");
            let mapping = mapping.clone();
            Box::pin(async move {
                if matches!(
                    state,
                    RTCPeerConnectionState::Closed | RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected
                ) {
                    mapping.shutdown();
                }
            })
        }));
    }
    {
        let (channels, file_channel) = (channels.clone(), file_channel.clone());
        let (outbox, terminal, mapping, sequence) = (outbox.clone(), terminal.clone(), mapping.clone(), sequence.clone());
        pc.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
            let label = channel.label().to_owned();
            log::info!("对端打开 data channel: {label}");
            channels.lock().unwrap().insert(label.clone(), channel.clone());
            if label == FILE_DATA_CHANNEL {
                *file_channel.lock().unwrap() = Some(channel.clone());
            }
            let (outbox, terminal, mapping, sequence) = (outbox.clone(), terminal.clone(), mapping.clone(), sequence.clone());
            channel.on_message(Box::new(move |msg: DataChannelMessage| {
                let (outbox, terminal, mapping, sequence) = (outbox.clone(), terminal.clone(), mapping.clone(), sequence.clone());
                let label = label.clone();
                Box::pin(async move {
                    dispatch(&label, msg.data.to_vec(), &outbox, &terminal, &mapping, &sequence).await;
                })
            }));
            Box::pin(async {})
        }));
    }

    pc.set_remote_description(RTCSessionDescription::offer(offer_sdp.to_owned()).map_err(|e| format!("offer 无效: {e}"))?)
        .await
        .map_err(|e| format!("设置远端 SDP 失败: {e}"))?;

    let answer = pc.create_answer(None).await.map_err(|e| format!("生成 answer 失败: {e}"))?;
    let media_section = negotiated_bundle_media(&answer.sdp).ok_or("answer 里没有可用的 BUNDLE 段")?;
    *bundle.lock().await = Some(media_section);
    pc.set_local_description(answer.clone())
        .await
        .map_err(|e| format!("设置本地 SDP 失败: {e}"))?;

    signaling
        .emit_soac(&Soac {
            kind: "answer".into(),
            sdp: Some(with_max_message_size(&answer.sdp)),
            ice_id: Some(Value::String(ice_id)),
            // 空 client_id 不能发出去：云端按它路由，空串会让对端收不到。
            client_id: (!client_id.is_empty()).then_some(client_id),
            app_control_id,
            ..Default::default()
        })
        .await?;

    Ok(pc)
}

/// 注入对端候选。`candidate` 既可能是字符串，也可能是 {candidate, sdpMid, sdpMLineIndex}。
pub async fn add_candidate(pc: &Arc<RTCPeerConnection>, soac: &Soac) -> Result<(), String> {
    let Some(value) = &soac.candidate else { return Ok(()) };
    let (candidate, mid, mline) = if let Some(text) = value.as_str() {
        (text.to_owned(), soac.sdp_mid.clone(), soac.sdp_mline_index)
    } else {
        (
            value.get("candidate").and_then(Value::as_str).unwrap_or_default().to_owned(),
            value
                .get("sdpMid")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| soac.sdp_mid.clone()),
            value
                .get("sdpMLineIndex")
                .and_then(Value::as_u64)
                .map(|i| i as i32)
                .or(soac.sdp_mline_index),
        )
    };
    if candidate.is_empty() {
        return Ok(());
    }
    pc.add_ice_candidate(RTCIceCandidateInit {
        candidate,
        sdp_mid: mid.or_else(|| Some("0".into())),
        sdp_mline_index: Some(mline.unwrap_or(0).max(0) as u16),
        username_fragment: None,
    })
    .await
    .map_err(|e| format!("添加远端候选失败: {e}"))
}

pub(crate) fn with_max_message_size(sdp: &str) -> String {
    if sdp.contains("a=max-message-size:") {
        return sdp.to_owned();
    }
    let eol = if sdp.contains("\r\n") { "\r\n" } else { "\n" };
    let mut out = String::with_capacity(sdp.len() + 32);
    for line in sdp.split_inclusive('\n') {
        out.push_str(line);
        if line.trim_end().starts_with("a=sctp-port:") {
            out.push_str(&format!("a=max-message-size:{MAX_MESSAGE_SIZE}{eol}"));
        }
    }
    out
}

/// 一个 peer 的所有出向报文：按通道名找到 data channel 再发。
/// 通道可能还没 Open（viewer 先发请求后开通道的时序），短暂等待而不丢。
async fn drain_outbox(
    mut rx: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    channels: Arc<StdMutex<HashMap<String, Arc<RTCDataChannel>>>>,
) {
    while let Some((label, message)) = rx.recv().await {
        let mut target = channels.lock().unwrap().get(&label).cloned();
        for _ in 0..100 {
            if target.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            target = channels.lock().unwrap().get(&label).cloned();
        }
        let Some(channel) = target else {
            log::warn!("输出被丢弃：{label} 未打开");
            continue;
        };
        if let Err(e) = channel.send(&bytes::Bytes::from(message)).await {
            log::warn!("{label} 发送失败: {e}");
        }
    }
}

/// 一条入向报文的去向：终端帧/终端 RPC → 终端；Message 27 → 端口映射。
async fn dispatch(
    label: &str,
    data: Vec<u8>,
    outbox: &mpsc::UnboundedSender<(String, Vec<u8>)>,
    terminal: &TerminalConn,
    mapping: &Arc<portmap::Session>,
    sequence: &AtomicU64,
) {
    // 终端是裸 "TERM" 帧，不是 protobuf。
    if label == BINARY_DATA_CHANNEL && terminal::is_frame(&data) {
        terminal.handle(&data);
        return;
    }
    let Ok(fields) = proto::read_fields(&data) else { return };
    // 终端 RPC（28 check_env / 29 rename_session）：Message 21 请求 → Message 22 应答，原通道返回。
    let response = fields
        .iter()
        .find_map(|f| match (f.number, &f.value) {
            (21, WireValue::Bytes(request)) => Some(*request),
            _ => None,
        })
        .and_then(|request| proto::read_fields(request).ok())
        .and_then(|request| terminal::rpc_response(&request));
    if let Some(body) = response {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let message = proto::envelope(sequence.fetch_add(1, Ordering::Relaxed), timestamp, 22, &body);
        let _ = outbox.send((label.to_owned(), message));
        return;
    }
    // 端口映射：Message 27，只接受 FILE_DATA_CHANNEL（Session 自己拒绝其他通道）。
    mapping.handle(&fields, label);
}

/// BUNDLE 组里第一个还在用的段的 (mid, mLineIndex)。
fn negotiated_bundle_media(sdp: &str) -> Option<(String, u16)> {
    let preferred: Vec<&str> = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=group:BUNDLE "))?
        .split_whitespace()
        .collect();
    let mut sections: Vec<(bool, Option<String>)> = Vec::new();
    for line in sdp.lines() {
        if let Some(media) = line.strip_prefix("m=") {
            let port = media.split_whitespace().nth(1)?;
            sections.push((port != "0", None));
        } else if let Some(mid) = line.strip_prefix("a=mid:") {
            if let Some(section) = sections.last_mut() {
                section.1 = Some(mid.to_owned());
            }
        }
    }
    for mid in preferred {
        for (index, (active, section_mid)) in sections.iter().enumerate() {
            if *active && section_mid.as_deref() == Some(mid) {
                return Some((mid.to_owned(), index.try_into().ok()?));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{negotiated_bundle_media, with_max_message_size};

    #[test]
    fn advertises_max_message_size_once() {
        let sdp = "v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=sctp-port:5000\r\n";
        let out = with_max_message_size(sdp);
        assert!(out.contains("a=sctp-port:5000\r\na=max-message-size:262144\r\n"));
        assert_eq!(with_max_message_size(&out), out);
    }

    #[test]
    fn picks_first_active_bundle_section() {
        let sdp = "a=group:BUNDLE 0 1\r\nm=audio 0 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\n";
        assert_eq!(negotiated_bundle_media(sdp), Some(("1".to_owned(), 1)));
        assert_eq!(negotiated_bundle_media("v=0\r\n"), None);
    }
}
