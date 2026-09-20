use blew::gatt::{AttributePermissions, CharacteristicProperties, GattCharacteristic};
use uuid::Uuid;

pub mod chat;

pub trait Buffer {
    const UUID: Uuid;
    const PROPERTIES: CharacteristicProperties;
    const PERMISSIONS: AttributePermissions;

    fn characteristics() -> GattCharacteristic {
        GattCharacteristic {
            uuid: Self::UUID,
            properties: Self::PROPERTIES,
            permissions: Self::PERMISSIONS,
            value: vec![],
            descriptors: vec![],
        }
    }
}
