# rtc-turn 0.21.1: stale nonce recovery

This directory contains `rtc-turn 0.21.1` from crates.io (MIT / Apache-2.0),
with two targeted changes in `src/client/relay.rs`:

- Keep a channel binding and its channel number after a ChannelBind response
  reports `438 Stale Nonce`. Reset an initial bind to Idle or a refresh to Ready,
  leaving its last successful refresh time unchanged. The next send retries with
  the updated nonce and original number, while inbound ChannelData remains usable.
  See <https://github.com/webrtc-rs/rtc/issues/267> and RFC 8656 section 12.
- Retry an allocation Refresh promptly after updating a stale nonce. Waiting an
  entire additional refresh interval can let the allocation expire.
  See <https://github.com/webrtc-rs/rtc/issues/263>.

The package manifest omits the original example and benchmark targets. Library
source is otherwise retained. Other RTC crates continue to come from crates.io.
This directory replaces no transport implementation and is unrelated to the
removed `vendor-src` submodule.

Regression tests in `src/net/turn_tests.rs` drive the public TURN client API with
synthetic server responses and injected time. All three fail against the original
crates.io 0.21.1 implementation and exercise the dependency used by both host and
controller. No cloud credentials or public TURN server are needed.

Remove this patch once a published compatible rtc-turn release includes both fixes.

Original crates.io archive SHA256:
`c53078a0696df5a3a5e4947bc550c5e903b59803b252f1d39062d637ebd3d17e`.
