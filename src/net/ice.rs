//! 每连接的 ICE 配置。STUN/TURN 由云端在 `be-controlled` 头的 `iceServers` 里下发，
//! 代码里不内置任何 UU 中继地址；云端不给时才退回公共 STUN。
//! 故意不实现 Debug：凭据不能进日志。

use serde_json::Value;
use webrtc::ice_transport::ice_credential_type::RTCIceCredentialType;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;

#[derive(Clone, Default)]
pub struct SessionIce {
    servers: Vec<RTCIceServer>,
    relay_only: bool,
}

impl SessionIce {
    /// 从 `be-controlled` 的 header（或 header.data）里取 `iceServers` / `force_relay`。
    pub fn from_header(header: &Value) -> Self {
        let data = header.get("data").unwrap_or(header);
        let field = |name: &str| header.get(name).or_else(|| data.get(name));
        let mut result = Self {
            relay_only: field("force_relay").and_then(Value::as_bool).unwrap_or(false),
            ..Default::default()
        };
        if let Some(servers) = field("iceServers").and_then(Value::as_array) {
            for server in servers.iter().take(16) {
                let urls: Vec<String> = match server.get("urls").or_else(|| server.get("url")) {
                    Some(Value::String(url)) => vec![url.clone()],
                    Some(Value::Array(urls)) => urls.iter().filter_map(Value::as_str).take(16).map(str::to_owned).collect(),
                    _ => Vec::new(),
                }
                .into_iter()
                .filter(|url| url.len() <= 2048 && ["stun:", "stuns:", "turn:", "turns:"].iter().any(|p| url.starts_with(p)))
                .collect();
                if urls.is_empty() {
                    continue;
                }
                let string = |name: &str| server.get(name).and_then(Value::as_str).filter(|s| s.len() <= 4096).unwrap_or("").to_owned();
                result.servers.push(RTCIceServer {
                    urls,
                    username: string("username"),
                    credential: string("credential"),
                    credential_type: RTCIceCredentialType::Password,
                });
            }
        }
        result
    }

    pub fn configuration(&self) -> Result<RTCConfiguration, String> {
        let has_turn = self
            .servers
            .iter()
            .any(|s| s.urls.iter().any(|u| u.starts_with("turn:") || u.starts_with("turns:")));
        // 要求中继却没有 TURN 时明确失败，不静默降级成直连。
        if self.relay_only && !has_turn {
            return Err("云端要求 relay-only，但没有可用的 TURN 配置".into());
        }
        let supplied = !self.servers.is_empty();
        let servers = if supplied {
            self.servers.clone()
        } else {
            vec![RTCIceServer {
                urls: vec![
                    "stun:stun.l.google.com:19302".into(),
                    "stun:stun1.l.google.com:19302".into(),
                    "stun:stun.miwifi.com:3478".into(),
                ],
                ..Default::default()
            }]
        };
        log::info!(
            "ICE 配置: cloud_supplied={supplied} server_count={} turn_available={has_turn} relay_only={}",
            servers.len(),
            self.relay_only
        );
        Ok(RTCConfiguration {
            ice_servers: servers,
            ice_transport_policy: if self.relay_only { RTCIceTransportPolicy::Relay } else { RTCIceTransportPolicy::All },
            ..Default::default()
        })
    }
}
