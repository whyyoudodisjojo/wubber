use blew::gatt::{AttributePermissions, CharacteristicProperties};
use uuid::uuid;

use crate::buffers::Buffer;

pub struct ChatBuffer;

impl Buffer for ChatBuffer {
    const PERMISSIONS: blew::gatt::AttributePermissions = AttributePermissions::WRITE;
    const PROPERTIES: blew::gatt::CharacteristicProperties =
        CharacteristicProperties::WRITE_WITHOUT_RESPONSE;
    const UUID: uuid::Uuid = uuid!("16fe48d4-6b5c-44c1-a331-d6c0ea61d714");
}
