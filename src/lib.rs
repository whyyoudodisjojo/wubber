pub mod buffers;
pub mod relays;
pub mod router;
pub mod services;
pub mod transport;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::mpsc::unbounded_channel;
use uuid::Uuid;

use crate::router::Router;
use crate::services::Services;
use crate::services::chat::ChatService;
use crate::services::discovery::Discovery;
use crate::transport::Transport;

pub const SERVICE_UUID: &str = "95ebaece-3ea2-4b13-b2ac-c48d6799a9a6";

#[derive(Serialize, Deserialize, Debug)]
pub struct Frame {
    pub origin: String,
    pub seq: u64,
    pub ttl: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(origin: String, seq: u64, ttl: u8, payload: Vec<u8>) -> Self {
        Self {
            origin,
            seq,
            ttl,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        ciborium::into_writer(self, &mut out)?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(ciborium::from_reader(bytes)?)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Chat { message: String, target: String },
    HeartBeat,
}

pub struct NetChat<T: Transport> {
    transport: Arc<T>,
}

impl<T: Transport> NetChat<T> {
    pub async fn new(transport: T) -> Result<Self> {
        Ok(Self {
            transport: Arc::new(transport),
        })
    }

    pub async fn start(self) -> Result<()> {
        self.transport.advertise().await?;

        let (chat_tx, chat_rx) = unbounded_channel();
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();

        Router::new(chat_tx, heartbeat_tx).start(self.transport.clone())?;

        let peers: Arc<Mutex<HashSet<T::Peer>>> = Arc::new(Mutex::new(HashSet::new()));

        let own_id = Uuid::new_v4().to_string();

        let sender = ChatService::new(self.transport.clone(), chat_rx, own_id);
        let discovery = Discovery::new(self.transport, peers, heartbeat_rx);

        tokio::spawn(async move {
            let _ = sender
                .run()
                .await
                .map_err(|e| eprintln!("chat service stopped: {e}"));
        });

        discovery.run().await?;

        Ok(())
    }
}
