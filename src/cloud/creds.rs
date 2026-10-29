//! 登录凭据：配置文件 + 环境变量，Debug 永远不打印内容。

use serde_json::Value;

/// 官方 PC 客户端（x-param-plat=1）内置的签名 key，所有用户相同。
pub const DEFAULT_SIGN_KEY: &str = "alWiSzXZTLu3WfFnw13uBru3";

#[derive(Clone, Default)]
pub struct Credentials {
    pub user_id: String,
    pub device_id: String,
    pub client_id: String,
    pub system_id: String,
    pub token: String,
    pub csrf: String,
    pub sign_key: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials { .. }")
    }
}

impl Credentials {
    /// 环境变量（UU_USER_ID / UU_DEVICE_ID / UU_CLIENT_ID / UU_SYSTEM_ID / UU_TOKEN /
    /// UU_CSRF / UU_SIGN_KEY）优先于配置文件。
    pub fn load(file: Option<&Value>) -> Result<Self, String> {
        let get = |key: &str| -> String {
            std::env::var(format!("UU_{}", key.to_uppercase()))
                .ok()
                .filter(|v| !v.is_empty())
                .or_else(|| file.and_then(|f| f.get(key)).and_then(Value::as_str).map(str::to_owned))
                .unwrap_or_default()
        };
        let creds = Self {
            user_id: get("user_id"),
            device_id: get("device_id"),
            client_id: get("client_id"),
            system_id: get("system_id"),
            token: get("token"),
            csrf: get("csrf"),
            sign_key: Some(get("sign_key")).filter(|k| !k.is_empty()).unwrap_or_else(|| DEFAULT_SIGN_KEY.into()),
        };
        let missing: Vec<&str> = [
            ("user_id", &creds.user_id),
            ("device_id", &creds.device_id),
            ("client_id", &creds.client_id),
            ("system_id", &creds.system_id),
            ("token", &creds.token),
            ("csrf", &creds.csrf),
        ]
        .iter()
        .filter(|(_, v)| v.is_empty())
        .map(|(k, _)| *k)
        .collect();
        if missing.is_empty() {
            Ok(creds)
        } else {
            Err(format!("缺少登录凭据: {}（先执行 `uu-minimal login`）", missing.join(", ")))
        }
    }
}
