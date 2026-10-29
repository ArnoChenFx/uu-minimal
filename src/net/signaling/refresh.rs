//! 房间重连凭证的刷新调度；Engine.IO ping/pong 不会替代此协议步骤。

use std::collections::VecDeque;
use std::time::Duration;
use serde_json::Value;
use tokio::time::Instant;

#[derive(Clone, Copy)]
pub(super) struct Timing {
    pub first: Duration,
    pub retry: Duration,
    pub success: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self { first: Duration::from_secs(30), retry: Duration::from_secs(30), success: Duration::from_secs(300) }
    }
}

pub(super) struct Refresh {
    timing: Timing,
    pub next: Option<Instant>,
    pending: VecDeque<u64>,
    accepted_id: u64,
    // 只属于当前房间；不持久化、不记录内容、不用于新房间鉴权。
    _reconnect_key: Option<String>,
}

impl Refresh {
    pub fn new(timing: Timing) -> Self {
        Self { timing, next: None, pending: VecDeque::new(), accepted_id: 0, _reconnect_key: None }
    }

    pub fn connected(&mut self, now: Instant) {
        if self.next.is_none() { self.next = Some(now + self.timing.first); }
    }

    pub fn sent(&mut self, id: u64, now: Instant) {
        // 没有 ACK 时也在 retry 后再次发送；限制保留的历史请求数量。
        if self.pending.len() == 16 { self.pending.pop_front(); }
        self.pending.push_back(id);
        self.next = Some(now + self.timing.retry);
    }

    /// None 表示不是当前窗口中的刷新请求，不能改变刷新计划。
    pub fn acknowledge(&mut self, id: u64, payload: &Value, now: Instant) -> Option<bool> {
        let index = self.pending.iter().position(|pending| *pending == id)?;
        self.pending.remove(index);
        let key = payload.as_array().filter(|items| items.first().and_then(Value::as_str) == Some("success"))
            .and_then(|items| items.get(1)).and_then(|body| body.get("reconnect_key"))
            .and_then(Value::as_str).filter(|key| !key.is_empty() && key.len() <= 16384);
        if let Some(key) = key {
            // 延迟的旧 ACK 不得覆盖已经接受的较新凭证和截止时间。
            if id > self.accepted_id {
                self.accepted_id = id;
                self._reconnect_key = Some(key.to_owned());
                self.next = Some(now + self.timing.success);
            }
        }
        Some(key.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn handshake_starts_first_refresh_and_success_schedules_five_minutes() {
        let now = Instant::now();
        let mut refresh = Refresh::new(Timing::default());
        assert!(refresh.next.is_none());
        refresh.connected(now);
        assert_eq!(refresh.next, Some(now + Duration::from_secs(30)));
        refresh.connected(now + Duration::from_secs(10));
        assert_eq!(refresh.next, Some(now + Duration::from_secs(30)));
        let sent = now + Duration::from_secs(30);
        refresh.sent(12, sent);
        assert_eq!(refresh.next, Some(sent + Duration::from_secs(30)));
        let ack = sent + Duration::from_secs(2);
        assert_eq!(refresh.acknowledge(12, &json!(["success", {"reconnect_key":"test-key"}]), ack), Some(true));
        assert_eq!(refresh.next, Some(ack + Duration::from_secs(300)));
    }

    #[test]
    fn invalid_and_unrelated_acks_do_not_postpone_retry() {
        let now = Instant::now();
        let mut refresh = Refresh::new(Timing::default());
        for payload in [json!(["error", {"reconnect_key":"test-key"}]), json!(["success"]),
            json!(["success", {"reconnect_key":""}]), json!(["success", {"reconnect_key":42}]),
            json!({"reconnect_key":"test-key"}), json!(["success", {"reconnect_key":"x".repeat(16385)}])] {
            refresh.sent(12, now);
            assert_eq!(refresh.acknowledge(11, &payload, now), None);
            assert_eq!(refresh.acknowledge(12, &payload, now), Some(false));
            assert_eq!(refresh.next, Some(now + Duration::from_secs(30)));
            assert!(refresh._reconnect_key.is_none());
        }
    }

    #[test]
    fn silence_keeps_retrying_with_bounded_request_history() {
        let now = Instant::now();
        let mut refresh = Refresh::new(Timing::default());
        for id in 12..40 { refresh.sent(id, now); }
        assert_eq!(refresh.pending.len(), 16);
        assert_eq!(refresh.acknowledge(12, &json!(["success", {"reconnect_key":"old"}]), now), None);
        assert_eq!(refresh.next, Some(now + Duration::from_secs(30)));
    }

    #[test]
    fn stale_acks_and_previous_rooms_cannot_replace_the_current_key() {
        let now = Instant::now();
        let timing = Timing::default();
        let mut refresh = Refresh::new(timing);
        refresh.connected(now);
        refresh.sent(12, now);
        refresh.sent(13, now + timing.retry);
        let ack_time = now + timing.retry;
        assert_eq!(refresh.acknowledge(13, &json!(["success", {"reconnect_key":"new"}]), ack_time), Some(true));
        let deadline = refresh.next;
        assert_eq!(refresh.acknowledge(12, &json!(["success", {"reconnect_key":"old"}]), ack_time + timing.retry), Some(true));
        assert_eq!(refresh._reconnect_key.as_deref(), Some("new"));
        assert_eq!(refresh.next, deadline);
        assert_eq!(refresh.acknowledge(13, &json!(["success", {"reconnect_key":"duplicate"}]), now), None);
        let mut next_room = Refresh::new(timing);
        assert!(next_room._reconnect_key.is_none());
        assert!(next_room.next.is_none());
        assert_eq!(next_room.acknowledge(13, &json!(["success", {"reconnect_key":"new"}]), now), None);
    }
}
