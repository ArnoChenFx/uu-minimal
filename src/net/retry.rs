//! 快速首轮恢复，持续失败或会话反复中断时逐渐退避。

use std::time::Duration;

pub(crate) struct Backoff {
    next: Duration,
}

impl Backoff {
    pub fn new() -> Self { Self { next: Duration::from_millis(500) } }

    pub fn immediate() -> Self { Self { next: Duration::ZERO } }

    pub fn delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = if delay.is_zero() { Duration::from_millis(500) }
            else { delay.saturating_mul(2).min(Duration::from_secs(30)) };
        delay
    }

    pub fn after_session(&mut self, lifetime: Duration) -> Duration {
        // 稳定运行后的首次重建不必固定等 3 秒；短命房间不能反复无间隔重建。
        if lifetime >= Duration::from_secs(30) { self.next = Duration::ZERO; }
        self.delay()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_short_sessions_back_off_but_a_stable_session_restores_fast_recovery() {
        let mut retry = Backoff::immediate();
        assert!(retry.after_session(Duration::from_secs(1)).is_zero());
        for _ in 0..20 {
            let delay = retry.after_session(Duration::from_secs(1));
            assert!(delay >= Duration::from_millis(500) && delay <= Duration::from_secs(30));
        }
        assert_eq!(retry.after_session(Duration::from_secs(1)), Duration::from_secs(30));
        assert!(retry.after_session(Duration::from_secs(60)).is_zero());
        assert_eq!(retry.after_session(Duration::from_secs(1)), Duration::from_millis(500));
    }

    #[test]
    fn viewer_retries_early_without_busy_looping_and_caps_persistent_failures() {
        let mut retry = Backoff::new();
        assert_eq!(retry.delay(), Duration::from_millis(500));
        assert_eq!(retry.delay(), Duration::from_secs(1));
        assert_eq!(retry.delay(), Duration::from_secs(2));
        for _ in 0..20 { assert!(retry.delay() <= Duration::from_secs(30)); }
        assert_eq!(retry.delay(), Duration::from_secs(30));
        // 每次重连过程独立，不能沿用上次失败到顶的等待时间。
        assert_eq!(Backoff::new().delay(), Duration::from_millis(500));
    }
}
