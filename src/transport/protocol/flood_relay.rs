use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures_util::stream::StreamExt;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::transport::{BroadcastEventStream, Incoming, Transport};

const MAX_TTL: u8 = 10;
const MAX_SEEN: usize = 8192;

pub struct FloodRelay<T: Transport> {
    inner: Arc<T>,
    own_id: String,
    seq: AtomicU64,
    ttl: u8,
    seen: Arc<Mutex<Seen>>,
    incoming_tx: broadcast::Sender<Incoming<T::Peer>>,
}

struct Seen {
    set: HashSet<(String, u64)>,
    order: VecDeque<(String, u64)>,
    cap: usize,
}

impl Seen {
    fn new(cap: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    fn insert(&mut self, key: (String, u64)) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        self.set.insert(key.clone());
        self.order.push_back(key);
        while self.order.len() > self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

impl<T: Transport> FloodRelay<T> {
    fn wrap(inner: Arc<T>, ttl: u8) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);
        Self {
            inner,
            own_id: Uuid::new_v4().to_string(),
            seq: AtomicU64::new(0),
            ttl,
            seen: Arc::new(Mutex::new(Seen::new(MAX_SEEN))),
            incoming_tx,
        }
    }

    fn spawn_pump(&self) {
        let inner = self.inner.clone();
        let seen = self.seen.clone();
        let own_id = self.own_id.clone();
        let incoming_tx = self.incoming_tx.clone();
        let mut incoming = inner.incoming();

        tokio::spawn(async move {
            while let Some((peer, bytes)) = incoming.next().await {
                let ttl = *bytes.first().unwrap_or(&0);
                if ttl == 0 {
                    continue;
                }
                let Some(seq) = bytes.get(1..9).and_then(|b| b.try_into().ok()) else {
                    continue;
                };
                let seq = u64::from_be_bytes(seq);
                let Some(origin_len) = bytes.get(9..11).and_then(|b| b.try_into().ok()) else {
                    continue;
                };
                let origin_len = u16::from_be_bytes(origin_len) as usize;
                let Some(origin) = bytes
                    .get(11..11 + origin_len)
                    .and_then(|b| std::str::from_utf8(b).ok())
                else {
                    continue;
                };
                if origin == own_id {
                    continue;
                }
                if !seen.lock().unwrap().insert((origin.to_string(), seq)) {
                    continue;
                }
                let payload = bytes[11 + origin_len..].to_vec();
                if ttl > 1 {
                    let mut out = Vec::with_capacity(1 + 8 + 2 + origin.len() + payload.len());
                    out.push(ttl - 1);
                    out.extend_from_slice(&seq.to_be_bytes());
                    out.extend_from_slice(&(origin.len() as u16).to_be_bytes());
                    out.extend_from_slice(origin.as_bytes());
                    out.extend_from_slice(&payload);
                    let _ = inner.send(&peer, &out).await;
                }
                if incoming_tx.send((peer, payload)).is_err() {
                    break;
                }
            }
        });
    }
}

impl<T: Transport> Transport for FloodRelay<T> {
    type Config = T::Config;
    type Peer = T::Peer;
    type Incoming = BroadcastEventStream<Incoming<T::Peer>>;
    type Discovered = T::Discovered;

    async fn new(config: Self::Config) -> Result<Self> {
        let inner = Arc::new(T::new(config).await?);
        let relay = Self::wrap(inner, MAX_TTL);
        relay.spawn_pump();
        Ok(relay)
    }

    async fn advertise(&self) -> Result<()> {
        self.inner.advertise().await
    }

    async fn scan(&self) -> Result<()> {
        self.inner.scan().await
    }

    fn discovered(&self) -> Self::Discovered {
        self.inner.discovered()
    }

    async fn connect(&self, peer: &Self::Peer) -> Result<()> {
        self.inner.connect(peer).await
    }

    async fn subscribe(&self, peer: &Self::Peer) -> Result<()> {
        self.inner.subscribe(peer).await
    }

    async fn disconnect(&self, peer: &Self::Peer) -> Result<()> {
        self.inner.disconnect(peer).await
    }

    async fn send(&self, peer: &Self::Peer, data: &[u8]) -> Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut out = Vec::with_capacity(1 + 8 + 2 + self.own_id.len() + data.len());
        out.push(self.ttl);
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&(self.own_id.len() as u16).to_be_bytes());
        out.extend_from_slice(self.own_id.as_bytes());
        out.extend_from_slice(data);
        self.inner.send(peer, &out).await
    }

    fn incoming(&self) -> Self::Incoming {
        self.incoming_tx.subscribe().into()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::sync::broadcast;

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Node(u8);

    impl fmt::Display for Node {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    struct Loop {
        incoming_tx: broadcast::Sender<Incoming<Node>>,
        discovered_tx: broadcast::Sender<Node>,
    }

    impl Transport for Loop {
        type Config = ();
        type Peer = Node;
        type Incoming = BroadcastEventStream<Incoming<Node>>;
        type Discovered = BroadcastEventStream<Node>;

        async fn new(_: ()) -> Result<Self> {
            let (incoming_tx, _) = broadcast::channel(16);
            let (discovered_tx, _) = broadcast::channel(16);
            Ok(Self {
                incoming_tx,
                discovered_tx,
            })
        }

        async fn advertise(&self) -> Result<()> {
            Ok(())
        }

        async fn scan(&self) -> Result<()> {
            Ok(())
        }

        fn discovered(&self) -> Self::Discovered {
            self.discovered_tx.subscribe().into()
        }

        async fn connect(&self, _: &Self::Peer) -> Result<()> {
            Ok(())
        }

        async fn subscribe(&self, _: &Self::Peer) -> Result<()> {
            Ok(())
        }

        async fn disconnect(&self, _: &Self::Peer) -> Result<()> {
            Ok(())
        }

        async fn send(&self, peer: &Self::Peer, data: &[u8]) -> Result<()> {
            let _ = self.incoming_tx.send((*peer, data.to_vec()));
            Ok(())
        }

        fn incoming(&self) -> Self::Incoming {
            self.incoming_tx.subscribe().into()
        }
    }

    fn framed(origin: &str, seq: u64, ttl: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(ttl);
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&(origin.len() as u16).to_be_bytes());
        out.extend_from_slice(origin.as_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[tokio::test]
    async fn relays_foreign_packet_once() {
        let inner = Loop::new(()).await.unwrap();
        let inner = Arc::new(inner);
        let relay = FloodRelay::wrap(inner.clone(), 5);
        relay.spawn_pump();

        let mut rx = relay.incoming();

        let bytes = framed("peer-2", 9, 4, b"hello");
        let _ = inner.incoming_tx.send((Node(2), bytes));

        let (peer, payload) = tokio::time::timeout(Duration::from_secs(1), rx.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer, Node(2));
        assert_eq!(payload, b"hello");

        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn drops_own_echo() {
        let inner = Loop::new(()).await.unwrap();
        let inner = Arc::new(inner);
        let relay = FloodRelay::wrap(inner.clone(), 5);
        relay.spawn_pump();

        let mut rx = relay.incoming();

        let bytes = framed(&relay.own_id, 1, 5, b"echo");
        let _ = inner.incoming_tx.send((Node(1), bytes));

        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.next())
                .await
                .is_err()
        );
    }
}
