use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use blew::central::{CentralEvent, ScanFilter, WriteType};
use blew::{Central, DeviceId};
use futures_util::stream::StreamExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::uuid;

use crate::buffers::Buffer;
use crate::buffers::chat::ChatBuffer;
use crate::services::Services;
use crate::{Message, NetChat};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Discovery {
    central: Arc<Central>,
    peers: Arc<Mutex<HashSet<DeviceId>>>,
    heartbeat_rx: Mutex<UnboundedReceiver<(DeviceId, Message)>>,
    own_id: String,
}

impl Discovery {
    pub fn new(
        central: Arc<Central>,
        peers: Arc<Mutex<HashSet<DeviceId>>>,
        heartbeat_rx: UnboundedReceiver<(DeviceId, Message)>,
        own_id: String,
    ) -> Self {
        Self {
            central,
            peers,
            heartbeat_rx: Mutex::new(heartbeat_rx),
            own_id,
        }
    }

    async fn next_heartbeat(&self) -> Option<(DeviceId, Message)> {
        self.heartbeat_rx.lock().await.recv().await
    }
}

impl Services for Discovery {
    async fn run(&self) -> Result<()> {
        let mut events = self.central.events();

        self.central
            .start_scan(ScanFilter {
                services: vec![uuid!(NetChat::SERVICE_UUID)],
                ..Default::default()
            })
            .await?;

        let mut last_seen: HashMap<DeviceId, Instant> = HashMap::new();
        let mut known_ids: HashMap<DeviceId, String> = HashMap::new();

        let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_timer.tick().await;

        loop {
            tokio::select! {
                Some(event) = events.next() => {
                    if let CentralEvent::DeviceDiscovered(device) = event {
                        if self.peers.lock().await.contains(&device.id) {
                            continue;
                        }

                        match self.central.connect(&device.id).await {
                            Ok(()) => {
                                println!("Connected to {}", device.id);
                                let _ = self
                                    .central
                                    .subscribe_characteristic(&device.id, ChatBuffer::UUID)
                                    .await
                                    .map_err(|e| eprintln!("Failed to subscribe to {}: {}", device.id, e));
                                self.peers.lock().await.insert(device.id.clone());
                                last_seen.insert(device.id.clone(), Instant::now());
                            }
                            Err(e) => println!("Connection failed: {}", e),
                        }
                    }
                }

                Some((device_id, message)) = self.next_heartbeat() => {
                    if let Message::HeartBeat { id } = message {
                        last_seen.insert(device_id.clone(), Instant::now());
                        if known_ids.get(&device_id) != Some(&id) {
                            known_ids.insert(device_id.clone(), id.clone());
                            println!("Peer {} has id {}", device_id, id);
                        }
                    }
                }

                _ = heartbeat_timer.tick() => {
                    let now = Instant::now();
                    let stale: Vec<DeviceId> = last_seen
                        .iter()
                        .filter(|(_, last)| now.duration_since(**last) > HEARTBEAT_TIMEOUT)
                        .map(|(id, _)| id.clone())
                        .collect();
                    for id in stale {
                        last_seen.remove(&id);
                        self.peers.lock().await.remove(&id);
                        let _ = self.central.disconnect(&id).await;
                        println!("Lost connection to {}", id);
                    }

                    let mut payload = Vec::new();
                    let _ = ciborium::into_writer(
                        &Message::HeartBeat { id: self.own_id.clone() },
                        &mut payload,
                    ).inspect_err(|e| eprintln!("Failed to encode cbor: {e}"));

                    let peers: Vec<DeviceId> = self.peers.lock().await.iter().cloned().collect();
                    for peer_id in peers {
                        let _ = self
                            .central
                            .write_characteristic(
                                &peer_id,
                                ChatBuffer::UUID,
                                payload.clone(),
                                WriteType::WithoutResponse,
                            )
                            .await.inspect_err(|e| eprintln!("Failed to launch hearbeat: {e}"));
                    }
                }
            }
        }
    }
}
