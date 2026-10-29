//! 被控端：`run` 的房间循环，以及终端与端口映射两条数据通道。

pub mod portmap;
pub mod terminal;

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::cloud::api::{Api, RoomError};
use crate::cloud::config;
use crate::cloud::creds::Credentials;
use crate::net::ice;
use crate::net::peer;
use crate::net::signaling::{Event, Signaling};

pub async fn run() -> Result<(), String> {
    let file = config::read()?;
    let creds = Credentials::load(file.as_ref())?;
    log::info!("已加载本机凭据（内容不打印）");
    let api = Arc::new(Api::new(creds));

    let mut stopping = false;
    let mut reconnect = crate::net::retry::Backoff::immediate();
    loop {
        log::info!("向云端申请房间凭据…");
        let room = match api.create_room().await {
            Ok(room) => room,
            Err(RoomError::LoginExpired) => return Err("登录已失效，请重新执行 `uu-minimal login`".into()),
            Err(RoomError::Other(e)) => {
                let delay = reconnect.delay();
                log::error!("建房失败: {e}，{}ms 后重试", delay.as_millis());
                if !wait_retry(delay).await { break; }
                continue;
            }
        };
        let server = room.server().to_owned();
        log::info!("拿到信令地址，开始 Socket.IO 握手");

        let (tx, mut rx) = mpsc::channel::<Event>(64);
        let signaling = match Signaling::connect(&server, &room.token, tx).await {
            Ok(signaling) => signaling,
            Err(e) => {
                let delay = reconnect.delay();
                log::error!("信令连接失败: {e}，{}ms 后重试", delay.as_millis());
                if !wait_retry(delay).await { break; }
                continue;
            }
        };

        let room_started = std::time::Instant::now();
        // 一个 ice_id = 一个 peer（桌面 / 文件 / 终端 / 端口映射各一条）。
        let mut pending_ice: HashMap<String, ice::SessionIce> = HashMap::new();
        let mut fallback_ice: Option<ice::SessionIce> = None;
        let mut peers: HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>> = HashMap::new();

        loop {
            let event = tokio::select! {
                _ = tokio::signal::ctrl_c() => { stopping = true; break; },
                event = rx.recv() => match event { Some(event) => event, None => break },
            };
            match event {
                Event::Connected => log::info!("信令就绪，等待遥控端接入"),
                Event::Connect { capture_type, ice_id, ice } => {
                    log::info!("准备接受连接: capture_type={capture_type} ice_id_present={}", ice_id.is_some());
                    match ice_id {
                        Some(id) => {
                            pending_ice.insert(id, ice);
                        }
                        None => fallback_ice = Some(ice),
                    }
                }
                Event::Offer(soac) => {
                    let ice_id = soac.ice_id.as_ref().map(value_text).unwrap_or_default();
                    let client_id = soac.client_id.clone().unwrap_or_default();
                    let transport = pending_ice.remove(&ice_id).or_else(|| fallback_ice.take()).unwrap_or_default();
                    let sdp = soac.sdp.clone().unwrap_or_default();
                    match peer::answer(&transport, &sdp, signaling.clone(), client_id, ice_id.clone(), soac.app_control_id.clone()).await {
                        Ok(pc) => {
                            if let Some(old) = peers.insert(ice_id.clone(), pc) {
                                close_peer(&old).await;
                            }
                            log::info!("已回复 answer: ice_id_present={} peers={}", !ice_id.is_empty(), peers.len());
                        }
                        Err(e) => log::error!("处理 offer 失败: {e}"),
                    }
                }
                Event::Candidate(soac) => {
                    let ice_id = soac.ice_id.as_ref().map(value_text).unwrap_or_default();
                    match peers.get(&ice_id) {
                        Some(pc) => {
                            if let Err(e) = peer::add_candidate(pc, &soac).await {
                                log::warn!("{e}");
                            }
                        }
                        None => log::warn!("收到未知 ice_id 的候选，忽略"),
                    }
                }
                Event::Released(id) => {
                    log::info!("遥控端断开（仅清理对应连接）");
                    pending_ice.remove(&id);
                    if let Some(pc) = peers.remove(&id) {
                        close_peer(&pc).await;
                    }
                }
                Event::Closed => {
                    log::warn!("信令断开，重建房间");
                    break;
                }
            }
        }

        let room_lifetime = room_started.elapsed();
        signaling.shutdown();
        futures_util::future::join_all(peers.drain().map(|(_, pc)| async move {
            close_peer(&pc).await;
        })).await;
        drop(signaling);
        if stopping { break; }
        let delay = reconnect.after_session(room_lifetime);
        if !delay.is_zero() {
            log::warn!("房间反复中断，{}ms 后重新建房", delay.as_millis());
            if !wait_retry(delay).await { break; }
        }
    }

    // Ctrl-C：交互式 shell 不响应 SIGTERM，这里显式收掉所有终端会话。
    terminal::shutdown_all();
    Ok(())
}

fn value_text(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())
}

async fn wait_retry(delay: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = tokio::signal::ctrl_c() => false,
    }
}

async fn close_peer(pc: &webrtc::peer_connection::RTCPeerConnection) {
    if tokio::time::timeout(Duration::from_secs(10), pc.close()).await.is_err() {
        log::warn!("关闭旧连接超时，继续恢复房间");
    }
}

// ---------------------------------------------------------------- 客户端子命令
