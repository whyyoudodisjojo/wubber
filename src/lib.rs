pub mod buffers;
pub mod router;
pub mod services;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use blew::gatt::{GattCharacteristic, GattService};
use blew::peripheral::AdvertisingConfig;
use blew::{Central, DeviceId, Peripheral};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use uuid::uuid;

use crate::buffers::Buffer;
use crate::buffers::chat::ChatBuffer;
use crate::router::Router;
use crate::services::Services;
use crate::services::chat::ChatService;
use crate::services::discovery::Discovery;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Chat {
        id: String,
        message: String,
        target: String,
        ttl: u8,
    },
    HeartBeat {
        id: String,
    },
}

pub struct ChatHandles {
    pub rx: UnboundedReceiver<(DeviceId, Message)>,
    pub heartbeat_rx: UnboundedReceiver<(DeviceId, Message)>,
}

pub struct Service {
    pub gatt_service: GattService,
    pub handles: ChatHandles,
    pub router: Router,
}

pub struct NetChat<T = ()> {
    pub peripheral: Arc<Peripheral>,
    pub service: T,
    pub characteristics: Vec<GattCharacteristic>,
}

impl NetChat {
    pub const SERVICE_UUID: &str = "95ebaece-3ea2-4b13-b2ac-c48d6799a9a6";
    pub async fn new() -> Result<Self> {
        let peripheral = Peripheral::new().await?;
        Ok(NetChat {
            peripheral: Arc::new(peripheral),
            characteristics: vec![],
            service: (),
        })
    }

    pub fn register_defaults(&mut self) {
        self.register_characteristics::<ChatBuffer>();
    }

    pub fn register_characteristics<T>(&mut self)
    where
        T: Buffer,
    {
        self.characteristics.push(T::characteristics());
    }

    pub async fn build(self) -> Result<NetChat<Service>> {
        let gatt_service = GattService {
            uuid: uuid!(NetChat::SERVICE_UUID),
            primary: true,
            characteristics: self.characteristics,
        };

        self.peripheral.add_service(&gatt_service).await?;

        let (chat_tx, chat_rx) = unbounded_channel();
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();

        let handles = ChatHandles {
            rx: chat_rx,
            heartbeat_rx,
        };

        let router = Router::new(chat_tx, heartbeat_tx);

        let service = Service {
            gatt_service,
            handles,
            router,
        };

        Ok(NetChat {
            peripheral: self.peripheral,
            characteristics: vec![],
            service,
        })
    }
}

impl NetChat<Service> {
    pub async fn start(self) -> Result<()> {
        let p_a = self.peripheral.clone();
        tokio::spawn(async move {
            p_a.clone()
                .start_advertising(&AdvertisingConfig {
                    local_name: "NetChat".to_string(),
                    service_uuids: vec![uuid!(NetChat::SERVICE_UUID)],
                })
                .await
        });

        self.service.router.start(self.peripheral.clone())?;

        let central: Arc<Central> = Arc::new(Central::new().await?);
        let peers: Arc<Mutex<HashSet<DeviceId>>> = Arc::new(Mutex::new(HashSet::new()));

        let ChatHandles { rx, heartbeat_rx } = self.service.handles;

        let own_id = uuid::Uuid::new_v4().to_string();

        let sender = ChatService::new(central.clone(), peers.clone(), rx, own_id.clone());
        let discovery = Discovery::new(central, peers, heartbeat_rx, own_id);

        tokio::spawn(async move {
            if let Err(e) = sender.run().await {
                eprintln!("chat service stopped: {e}");
            }
        });

        discovery.run().await?;

        Ok(())
    }
}
