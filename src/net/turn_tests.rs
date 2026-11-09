//! Exercise the actual TURN dependency with synthetic replies and injected time.
//! No account, network server, or waiting for a real nonce to expire is needed.

use bytes::BytesMut;
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtc::stun::attributes::{ATTR_NONCE, ATTR_REALM};
use rtc::stun::error_code::{ErrorCodeAttribute, CODE_STALE_NONCE, CODE_UNAUTHORIZED};
use rtc::stun::message::*;
use rtc::stun::textattrs::{Nonce, Realm};
use rtc::turn::client::{Client, ClientConfig, Event};
use rtc::turn::proto::{chandata::ChannelData, channum::ChannelNumber, lifetime::Lifetime, relayaddr::RelayedAddress};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

struct Fixture {
    client: Client,
    local: SocketAddr,
    server: SocketAddr,
    relay: SocketAddr,
    peer: SocketAddr,
    start: Instant,
}

impl Fixture {
    fn new() -> Self {
        let local = "127.0.0.1:40000".parse().unwrap();
        let server: SocketAddr = "127.0.0.1:3478".parse().unwrap();
        let relay: SocketAddr = "192.0.2.1:50000".parse().unwrap();
        let peer = "192.0.2.2:50001".parse().unwrap();
        let client = Client::new(ClientConfig {
            local_addr: local, turn_serv_addr: server.to_string(),
            username: "test-user".into(), password: "test-password".into(),
            ..Default::default()
        }, rtc::crypto::default_provider().unwrap()).unwrap();
        let mut f = Self { client, local, server, relay, peer, start: Instant::now() };
        f.client.allocate(f.start).unwrap();
        let anonymous = f.request(METHOD_ALLOCATE);
        f.error(0, &anonymous, CODE_UNAUTHORIZED, "nonce-1");
        let authenticated = f.request(METHOD_ALLOCATE);
        let mut response = Message::new();
        response.build(&[
            Box::new(authenticated.transaction_id),
            Box::new(MessageType::new(METHOD_ALLOCATE, CLASS_SUCCESS_RESPONSE)),
            Box::new(RelayedAddress { ip: relay.ip(), port: relay.port() }),
            Box::new(Lifetime(Duration::from_secs(600))),
        ]).unwrap();
        f.receive(0, &response.raw).unwrap();
        assert!(matches!(f.client.poll_event(), Some(Event::AllocateResponse(_, addr)) if addr == relay));
        f.client.relay(relay).unwrap().create_permission(f.start, peer).unwrap();
        let permission = f.request(METHOD_CREATE_PERMISSION);
        f.success(0, &permission);
        while f.client.poll_event().is_some() {}
        f
    }

    fn at(&self, seconds: u64) -> Instant { self.start + Duration::from_secs(seconds) }

    fn receive(&mut self, seconds: u64, bytes: &[u8]) -> rtc::shared::error::Result<()> {
        self.client.handle_read(TaggedBytesMut {
            now: self.at(seconds), message: BytesMut::from(bytes),
            transport: TransportContext { local_addr: self.local, peer_addr: self.server,
                transport_protocol: TransportProtocol::UDP, ecn: None },
        })
    }

    fn request(&mut self, method: Method) -> Message {
        while let Some(packet) = self.client.poll_write() {
            if !is_stun_message(&packet.message) { continue; }
            let mut message = Message::new();
            message.raw = packet.message.to_vec();
            message.decode().unwrap();
            if message.typ == MessageType::new(method, CLASS_REQUEST) { return message; }
        }
        panic!("expected a new {method:?} request");
    }

    fn success(&mut self, seconds: u64, request: &Message) {
        let mut response = Message::new();
        response.build(&[Box::new(request.transaction_id),
            Box::new(MessageType::new(request.typ.method, CLASS_SUCCESS_RESPONSE))]).unwrap();
        self.receive(seconds, &response.raw).unwrap();
    }

    fn error(&mut self, seconds: u64, request: &Message, code: rtc::stun::error_code::ErrorCode, nonce: &str) {
        let mut response = Message::new();
        response.build(&[
            Box::new(request.transaction_id),
            Box::new(MessageType::new(request.typ.method, CLASS_ERROR_RESPONSE)),
            Box::new(ErrorCodeAttribute { code, reason: Vec::new() }),
            Box::new(Realm::new(ATTR_REALM, "test-realm".into())),
            Box::new(Nonce::new(ATTR_NONCE, nonce.into())),
        ]).unwrap();
        self.receive(seconds, &response.raw).unwrap();
    }

    fn send(&mut self, seconds: u64) {
        let now = self.at(seconds);
        self.client.relay(self.relay).unwrap().send_to(now, b"outbound", self.peer).unwrap();
    }

    fn bind(&mut self) -> u16 {
        self.send(0);
        let request = self.request(METHOD_CHANNEL_BIND);
        let number = channel_number(&request);
        self.success(0, &request);
        while self.client.poll_write().is_some() {}
        number
    }
}

fn channel_number(request: &Message) -> u16 {
    let mut number = ChannelNumber::default();
    number.get_from(request).unwrap();
    number.0
}

#[test]
fn channel_bind_stale_nonce_keeps_receiving_and_retries_the_same_number() {
    // RFC 8656 section 12: a bound peer cannot be rebound to a different channel.
    // rtc-turn#267: deleting the binding on 438 drops inbound ChannelData and
    // makes every subsequent rebind fail with 400 on a compliant TURN server.
    let mut f = Fixture::new();
    let original = f.bind();
    f.send(301);
    let refresh = f.request(METHOD_CHANNEL_BIND);
    assert_eq!(channel_number(&refresh), original);
    f.error(301, &refresh, CODE_STALE_NONCE, "nonce-2");

    let mut inbound = ChannelData { number: ChannelNumber(original), data: b"inbound".to_vec(), ..Default::default() };
    inbound.encode();
    f.receive(301, &inbound.raw).expect("an expired nonce must not delete a live receive binding");
    assert!(matches!(f.client.poll_event(), Some(Event::DataIndicationOrChannelData(Some(ch), peer, data))
        if ch.0 == original && peer == f.peer && data.as_ref() == b"inbound"));

    f.send(302);
    let retry = f.request(METHOD_CHANNEL_BIND);
    assert_eq!(channel_number(&retry), original);
    assert_eq!(Nonce::get_from_as(&retry, ATTR_NONCE).unwrap().text, "nonce-2");
    f.success(302, &retry);
    f.send(303);
}

#[test]
fn first_channel_bind_stale_nonce_retries_with_the_updated_nonce() {
    let mut f = Fixture::new();
    f.send(0);
    let first = f.request(METHOD_CHANNEL_BIND);
    f.error(0, &first, CODE_STALE_NONCE, "nonce-2");
    f.send(1);
    let retry = f.request(METHOD_CHANNEL_BIND);
    assert_eq!(channel_number(&retry), channel_number(&first));
    assert_eq!(Nonce::get_from_as(&retry, ATTR_NONCE).unwrap().text, "nonce-2");
    f.success(1, &retry);
}

#[test]
fn allocation_stale_nonce_retries_before_the_allocation_can_expire() {
    let mut f = Fixture::new();
    f.client.handle_timeout(f.at(300)).unwrap();
    let refresh = f.request(METHOD_REFRESH);
    f.error(300, &refresh, CODE_STALE_NONCE, "nonce-2");
    // rtc-turn#263: waiting another full refresh interval loses the allocation.
    assert_eq!(f.client.poll_timeout(), Some(f.at(300)));
    f.client.handle_timeout(f.at(300)).unwrap();
    let retry = f.request(METHOD_REFRESH);
    assert_eq!(Nonce::get_from_as(&retry, ATTR_NONCE).unwrap().text, "nonce-2");
}
