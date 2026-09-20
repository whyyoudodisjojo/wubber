use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::stream::StreamExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::Message;
use crate::services::Services;
use crate::transport::Transport;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Discovery<T: Transport> {
    transport: Arc<T>,
    peers: Arc<Mutex<HashSet<T::Peer>>>,
    heartbeat_rx: Mutex<UnboundedReceiver<(T::Peer, Message)>>,
}

impl<T: Transport> Discovery<T> {
    pub fn new(
        transport: Arc<T>,
        peers: Arc<Mutex<HashSet<T::Peer>>>,
        heartbeat_rx: UnboundedReceiver<(T::Peer, Message)>,
    ) -> Self {
        Self {
            transport,
            peers,
            heartbeat_rx: Mutex::new(heartbeat_rx),
        }
    }

    async fn next_heartbeat(&self) -> Option<(T::Peer, Message)> {
        self.heartbeat_rx.lock().await.recv().await
    }
}

impl<T: Transport> Services for Discovery<T> {
    async fn run(&self) -> Result<()> {
        self.transport.scan().await?;
        let mut discovered = self.transport.discovered();

        let mut last_seen: HashMap<T::Peer, Instant> = HashMap::new();

        let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_timer.tick().await;

        loop {
            tokio::select! {
                Some(peer) = discovered.next() => {
                    if self.peers.lock().await.contains(&peer) {
                        continue;
                    }

                    match self.transport.connect(&peer).await {
                        Ok(()) => {
                            println!("Connected to {}", peer);
                            let _ = self
                                .transport
                                .subscribe(&peer)
                                .await
                                .map_err(|e| eprintln!("Failed to subscribe to {}: {}", peer, e));
                            self.peers.lock().await.insert(peer.clone());
                            last_seen.insert(peer, Instant::now());
                        }
                        Err(e) => println!("Connection failed: {}", e),
                    }
                }

                Some((device_id, _)) = self.next_heartbeat() => {
                    last_seen.insert(device_id, Instant::now());
                }

                _ = heartbeat_timer.tick() => {
                    let now = Instant::now();
                    let stale: Vec<T::Peer> = last_seen
                        .iter()
                        .filter(|(_, last)| now.duration_since(**last) > HEARTBEAT_TIMEOUT)
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in stale {
                        last_seen.remove(&id);
                        self.peers.lock().await.remove(&id);
                        let _ = self.transport.disconnect(&id).await;
                        println!("Lost connection to {}", id);
                    }

                    let mut payload = Vec::new();
                    let _ = ciborium::into_writer(&Message::HeartBeat, &mut payload)
                        .map_err(|e| eprintln!("Failed to encode cbor: {e}"));
                    let _ = self
                        .transport
                        .broadcast(&payload)
                        .await
                        .map_err(|e| eprintln!("Failed to launch heartbeat: {e}"));
                }
            }
        }
    }
}
