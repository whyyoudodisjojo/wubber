use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use futures_util::stream::{StreamExt, select_all};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::Frame;
use crate::transport::{BroadcastEventStream, Incoming, Transport};

const MAX_TTL: u8 = 10;
const MAX_SEEN: usize = 8192;

pub struct FloodRelay<T: Transport> {
    inners: Arc<Mutex<Vec<Arc<T>>>>,
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
    pub fn wrap(inners: Vec<Arc<T>>, ttl: u8) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);
        Self {
            inners: Arc::new(Mutex::new(inners)),
            own_id: Uuid::new_v4().to_string(),
            seq: AtomicU64::new(0),
            ttl,
            seen: Arc::new(Mutex::new(Seen::new(MAX_SEEN))),
            incoming_tx,
        }
    }

    pub fn inners_arc(&self) -> Vec<Arc<T>> {
        self.inners.lock().unwrap().clone()
    }

    fn spawn_inner_pump(&self, inner: Arc<T>) {
        let seen = self.seen.clone();
        let own_id = self.own_id.clone();
        let incoming_tx = self.incoming_tx.clone();
        let inners = self.inners.clone();
        let mut incoming = inner.incoming();

        tokio::spawn(async move {
            while let Some((peer, bytes)) = incoming.next().await {
                let Ok(mut frame) = Frame::decode(&bytes) else {
                    continue;
                };
                if frame.origin == own_id {
                    continue;
                }
                if !seen
                    .lock()
                    .unwrap()
                    .insert((frame.origin.clone(), frame.seq))
                {
                    continue;
                }
                if frame.ttl > 1 {
                    frame.ttl -= 1;
                    let Ok(out) = frame.encode() else {
                        continue;
                    };
                    let snapshot = inners.lock().unwrap().clone();
                    for inner in snapshot {
                        let _ = inner.send(&peer, &out).await;
                    }
                }
                if incoming_tx.send((peer, frame.payload)).is_err() {
                    break;
                }
            }
        });
    }

    pub fn spawn_pump(&self) {
        for inner in self.inners_arc() {
            self.spawn_inner_pump(inner);
        }
    }

    pub fn with_inner_transports(inners: Vec<Arc<T>>) -> Self {
        let relay = Self::wrap(inners, MAX_TTL);
        relay.spawn_pump();
        relay
    }

    pub fn add_transport(&self, inner: Arc<T>) {
        self.inners.lock().unwrap().push(inner.clone());
        self.spawn_inner_pump(inner);
    }

    pub fn transports(&self) -> Vec<Arc<T>> {
        self.inners_arc()
    }

    pub async fn first_success<'a, F, Fut>(&'a self, peer: &'a T::Peer, call: F) -> Result<()>
    where
        F: Fn(Arc<T>, &'a T::Peer) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let mut last = None;
        for inner in self.inners_arc() {
            match call(inner, peer).await {
                Ok(()) => return Ok(()),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("no inner transports")))
    }
}

impl<T: Transport> Transport for FloodRelay<T> {
    type Config = T::Config;
    type Peer = T::Peer;
    type Incoming = BroadcastEventStream<Incoming<T::Peer>>;
    type Discovered = futures_util::stream::SelectAll<T::Discovered>;

    async fn new(config: Self::Config) -> Result<Self> {
        let relay = Self::wrap(vec![Arc::new(T::new(config).await?)], MAX_TTL);
        relay.spawn_pump();
        Ok(relay)
    }

    async fn advertise(&self) -> Result<()> {
        for inner in self.inners_arc() {
            inner.advertise().await?;
        }
        Ok(())
    }

    async fn scan(&self) -> Result<()> {
        for inner in self.inners_arc() {
            inner.scan().await?;
        }
        Ok(())
    }

    fn discovered(&self) -> Self::Discovered {
        select_all(
            self.inners_arc()
                .into_iter()
                .map(|inner| inner.discovered()),
        )
    }

    fn incoming(&self) -> Self::Incoming {
        self.incoming_tx.subscribe().into()
    }

    async fn connect(&self, peer: &Self::Peer) -> Result<()> {
        self.first_success(peer, |inner, peer| async move { inner.connect(peer).await })
            .await
    }

    async fn subscribe(&self, peer: &Self::Peer) -> Result<()> {
        self.first_success(
            peer,
            |inner, peer| async move { inner.subscribe(peer).await },
        )
        .await
    }

    async fn disconnect(&self, peer: &Self::Peer) -> Result<()> {
        for inner in self.inners_arc() {
            let _ = inner.disconnect(peer).await;
        }
        Ok(())
    }

    async fn send(&self, _peer: &Self::Peer, data: &[u8]) -> Result<()> {
        self.broadcast(data).await
    }

    async fn broadcast(&self, data: &[u8]) -> Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let out = Frame::new(self.own_id.clone(), seq, self.ttl, data.to_vec()).encode()?;
        for inner in self.inners_arc() {
            inner.broadcast(&out).await?;
        }
        Ok(())
    }
}
