use std::sync::Arc;

use anyhow::{Result, anyhow};
use blew::central::{CentralEvent, ScanFilter, WriteType};
use blew::gatt::{GattCharacteristic, GattService};
use blew::peripheral::{AdvertisingConfig, LocalName, PeripheralRequest};
use blew::{Central, DeviceId, Peripheral};
use futures_util::stream::StreamExt;
use tokio::sync::broadcast;
use uuid::Uuid;

use super::{BroadcastEventStream, Incoming, Transport};

pub struct BleTransportConfig {
    pub name: String,
    pub service_uuid: Uuid,
    pub characteristics: Vec<GattCharacteristic>,
}

pub struct BleTransport {
    peripheral: Arc<Peripheral>,
    central: Arc<Central>,
    name: String,
    service_uuid: Uuid,
    channel_uuid: Uuid,
    incoming_tx: broadcast::Sender<Incoming<DeviceId>>,
    discovered_tx: broadcast::Sender<DeviceId>,
}

impl BleTransport {
    pub async fn new(config: BleTransportConfig) -> Result<Self> {
        let peripheral = Arc::new(Peripheral::new().await?);
        let central = Arc::new(Central::new().await?);

        let service = GattService {
            uuid: config.service_uuid,
            primary: true,
            characteristics: config.characteristics.clone(),
        };
        peripheral.add_service(&service).await?;

        let channel_uuid = config
            .characteristics
            .first()
            .map(|c| c.uuid)
            .ok_or_else(|| anyhow!("transport requires at least one characteristic"))?;

        let (incoming_tx, _) = broadcast::channel(1024);
        let (discovered_tx, _) = broadcast::channel(1024);

        {
            let incoming_tx = incoming_tx.clone();
            let discovered_tx = discovered_tx.clone();
            let peripheral = peripheral.clone();
            let central = central.clone();

            tokio::spawn(async move {
                let mut requests = match peripheral.take_requests() {
                    Some(requests) => requests,
                    None => {
                        eprintln!("transport: could not acquire peripheral request stream");
                        return;
                    }
                };

                let mut events = central.events();

                loop {
                    tokio::select! {
                        request = requests.next() => {
                            match request {
                                Some(PeripheralRequest::Write {
                                    char_uuid,
                                    value,
                                    responder,
                                    client_id,
                                    ..
                                }) if char_uuid == channel_uuid => {
                                    let _ = incoming_tx.send((client_id, value));
                                    if let Some(responder) = responder {
                                        responder.success();
                                    }
                                }
                                Some(_) => {}
                                None => break,
                            }
                        }

                        event = events.next() => {
                            match event {
                                Some(CentralEvent::DeviceDiscovered(device)) => {
                                    let _ = discovered_tx.send(device.id);
                                }
                                Some(CentralEvent::CharacteristicNotification {
                                    device_id,
                                    char_uuid,
                                    value,
                                }) if char_uuid == channel_uuid => {
                                    let _ = incoming_tx.send((device_id, value.to_vec()));
                                }
                                Some(_) => {}
                                None => break,
                            }
                        }
                    }
                }
            });
        }

        Ok(Self {
            peripheral,
            central,
            name: config.name,
            service_uuid: config.service_uuid,
            channel_uuid,
            incoming_tx,
            discovered_tx,
        })
    }
}

impl Transport for BleTransport {
    type Config = BleTransportConfig;
    type Peer = DeviceId;
    type Incoming = BroadcastEventStream<Incoming<DeviceId>>;
    type Discovered = BroadcastEventStream<DeviceId>;

    async fn new(config: Self::Config) -> Result<Self> {
        Self::new(config).await
    }

    async fn advertise(&self) -> Result<()> {
        self.peripheral
            .start_advertising(&AdvertisingConfig {
                local_name: LocalName::Temporary(self.name.clone()),
                service_uuids: vec![self.service_uuid],
            })
            .await?;
        Ok(())
    }

    async fn scan(&self) -> Result<()> {
        self.central
            .start_scan(ScanFilter {
                services: vec![self.service_uuid],
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    fn discovered(&self) -> Self::Discovered {
        self.discovered_tx.subscribe().into()
    }

    fn incoming(&self) -> Self::Incoming {
        self.incoming_tx.subscribe().into()
    }

    async fn connect(&self, peer: &Self::Peer) -> Result<()> {
        self.central.connect(peer).await?;
        Ok(())
    }

    async fn subscribe(&self, peer: &Self::Peer) -> Result<()> {
        self.central
            .subscribe_characteristic(peer, self.channel_uuid)
            .await?;
        Ok(())
    }

    async fn send(&self, peer: &Self::Peer, data: &[u8]) -> Result<()> {
        self.central
            .write_characteristic(
                peer,
                self.channel_uuid,
                data.to_vec(),
                WriteType::WithoutResponse,
            )
            .await?;
        Ok(())
    }

    async fn broadcast(&self, _data: &[u8]) -> Result<()> {
        Err(anyhow!("ble transport has no broadcast channel"))
    }

    async fn disconnect(&self, peer: &Self::Peer) -> Result<()> {
        self.central.disconnect(peer).await?;
        Ok(())
    }
}
