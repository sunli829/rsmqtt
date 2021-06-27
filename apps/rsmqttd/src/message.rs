use std::time::{Duration, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use mqttv5::{LastWill, Publish, PublishProperties, Qos};

#[derive(Debug, Clone)]
pub struct Message {
    pub publisher: Option<ByteString>,
    pub topic: ByteString,
    pub qos: Qos,
    pub payload: Bytes,
    pub retain: bool,
    pub expired_at: Option<SystemTime>,
    pub properties: PublishProperties,
}

impl Message {
    #[inline]
    pub fn is_expired(&self) -> bool {
        match &self.expired_at {
            Some(expired_at) => expired_at < &SystemTime::now(),
            None => false,
        }
    }

    #[inline]
    pub fn message_expiry_interval(&self) -> Option<(bool, u32)> {
        let now = SystemTime::now();
        match &self.expired_at {
            Some(expired_at) => match expired_at.duration_since(now) {
                Ok(duration) => Some((true, duration.as_secs() as u32)),
                Err(_) => Some((false, 0)),
            },
            None => None,
        }
    }

    #[inline]
    pub fn from_last_will(last_will: LastWill) -> Self {
        Self {
            publisher: None,
            topic: last_will.topic,
            qos: last_will.qos,
            payload: last_will.payload,
            retain: last_will.retain,
            expired_at: last_will
                .properties
                .message_expiry_interval
                .map(|n| SystemTime::now() + Duration::from_secs(n as u64)),
            properties: PublishProperties {
                correlation_data: last_will.properties.correlation_data,
                user_properties: last_will.properties.user_properties,
                content_type: last_will.properties.content_type,
                subscription_identifiers: Vec::new(),
                ..Default::default()
            },
        }
    }

    #[inline]
    pub fn from_publish(publisher: Option<ByteString>, publish: Publish) -> Self {
        let message_expired_at = {
            publish
                .properties
                .message_expiry_interval
                .map(|secs| SystemTime::now() + Duration::from_secs(secs as u64))
        };

        Self {
            publisher,
            topic: publish.topic,
            qos: publish.qos,
            payload: publish.payload,
            retain: publish.retain,
            expired_at: message_expired_at,
            properties: PublishProperties {
                payload_format_indicator: publish.properties.payload_format_indicator,
                response_topic: publish.properties.response_topic,
                correlation_data: publish.properties.correlation_data,
                user_properties: publish.properties.user_properties,
                content_type: publish.properties.content_type,
                ..Default::default()
            },
        }
    }

    #[inline]
    pub fn to_publish(&self) -> Publish {
        Publish {
            dup: false,
            qos: self.qos,
            retain: self.retain,
            topic: self.topic.clone(),
            packet_id: None,
            properties: self.properties.clone(),
            payload: self.payload.clone(),
        }
    }
}
