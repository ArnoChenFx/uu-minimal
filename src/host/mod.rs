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

    'rooms: loop {
        log::info!("向云端申请房间凭据…");
        let room = match api.create_room().await {
            Ok(room) => room,
            Err(RoomError::LoginExpired) => return Err("登录已失效，请重新执行 `uu-minimal login`".into()),
            Err(RoomError::Other(e)) => {
                log::error!("建房失败: {e}，5s 后重试");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let server = room.server().to_owned();
        log::info!("拿到信令地址，开始 Socket.IO 握手");

        let (tx, mut rx) = mpsc::channel::<Event>(64);
        let signaling = match Signaling::connect(&server, &room.token, tx).await {
            Ok(signaling) => signaling,
            Err(e) => {
                log::error!("信令连接失败: {e}，5s 后重试");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        // 一个 ice_id = 一个 peer（桌面 / 文件 / 终端 / 端口映射各一条）。
        let mut pending_ice: HashMap<String, ice::SessionIce> = HashMap::new();
        let mut fallback_ice: Option<ice::SessionIce> = None;
        let mut peers: HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>> = HashMap::new();

        loop {
            let event = tokio::select! {
                _ = tokio::signal::ctrl_c() => break 'rooms,
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
                                let _ = old.close().await;
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
                Event::Released(data) => {
                    let ice_id = ["ice_id", "iceId"].iter().find_map(|k| data.get(*k)).and_then(Value::as_str).map(str::to_owned);
                    log::info!("遥控端断开: ice_id_present={}", ice_id.is_some());
                    let closing: Vec<String> = match ice_id {
                        Some(id) if peers.contains_key(&id) => vec![id],
                        Some(_) => Vec::new(),
                        None => peers.keys().cloned().collect(),
                    };
                    for id in closing {
                        if let Some(pc) = peers.remove(&id) {
                            let _ = pc.close().await;
                        }
                    }
                }
                Event::Closed => {
                    log::warn!("信令断开，重建房间");
                    break;
                }
            }
        }

        for (_, pc) in peers.drain() {
            let _ = pc.close().await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Ctrl-C：交互式 shell 不响应 SIGTERM，这里显式收掉所有终端会话。
    terminal::shutdown_all();
    Ok(())
}

fn value_text(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())
}

// ---------------------------------------------------------------- 客户端子命令
