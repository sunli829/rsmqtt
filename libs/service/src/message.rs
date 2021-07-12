use std::time::{Duration, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use codec::{LastWill, Publish, PublishProperties, Qos};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    from_client_id: Option<ByteString>,
    from_uid: Option<ByteString>,
    created_at: SystemTime,
    topic: ByteString,
    qos: Qos,
    payload: Bytes,
    retain: bool,
    properties: PublishProperties,
}

impl Message {
    #[inline]
    pub fn new(topic: ByteString, qos: Qos, payload: Bytes) -> Self {
        Self {
            from_client_id: None,
            from_uid: None,
            created_at: SystemTime::now(),
            topic,
            qos,
            payload,
            retain: false,
            properties: PublishProperties::default(),
        }
    }

    #[inline]
    pub fn with_properties(mut self, properties: PublishProperties) -> Self {
        self.properties = properties;
        self
    }

    #[inline]
    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    #[inline]
    pub fn with_from_client_id(mut self, client_id: impl Into<ByteString>) -> Self {
        self.from_client_id = Some(client_id.into());
        self
    }

    #[inline]
    pub fn with_from_uid(mut self, uid: impl Into<ByteString>) -> Self {
        self.from_uid = Some(uid.into());
        self
    }

    #[inline]
    pub fn from_client_id(&self) -> Option<&ByteString> {
        self.from_client_id.as_ref()
    }

    #[inline]
    pub fn from_uid(&self) -> Option<&ByteString> {
        self.from_uid.as_ref()
    }

    #[inline]
    pub fn topic(&self) -> &ByteString {
        &self.topic
    }

    #[inline]
    pub fn qos(&self) -> Qos {
        self.qos
    }

    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[inline]
    pub fn properties(&self) -> &PublishProperties {
        &self.properties
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        self.retain
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        if let Some(message_expiry_interval) = self.properties.message_expiry_interval {
            let expired_at = self.created_at + Duration::from_secs(message_expiry_interval as u64);
            return expired_at <= SystemTime::now();
        }
        false
    }

    #[inline]
    pub fn from_last_will(last_will: LastWill) -> Self {
        let properties = PublishProperties {
            payload_format_indicator: last_will.properties.payload_format_indicator,
            message_expiry_interval: last_will.properties.message_expiry_interval,
            response_topic: last_will.properties.response_topic,
            correlation_data: last_will.properties.correlation_data,
            user_properties: last_will.properties.user_properties,
            content_type: last_will.properties.content_type,
            ..PublishProperties::default()
        };

        Self::new(last_will.topic, last_will.qos, last_will.payload)
            .with_retain(last_will.retain)
            .with_properties(properties)
    }

    #[inline]
    pub fn from_publish(publish: &Publish) -> Self {
        let properties = PublishProperties {
            payload_format_indicator: publish.properties.payload_format_indicator,
            message_expiry_interval: publish.properties.message_expiry_interval,
            response_topic: publish.properties.response_topic.clone(),
            correlation_data: publish.properties.correlation_data.clone(),
            user_properties: publish.properties.user_properties.clone(),
            content_type: publish.properties.content_type.clone(),
            ..PublishProperties::default()
        };

        Self::new(publish.topic.clone(), publish.qos, publish.payload.clone())
            .with_retain(publish.retain)
            .with_properties(properties)
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

    /// Create a Publish packet and update the message expiry interval `properties.message_expiry_interval`.
    ///
    /// Returns `None` if this message has expired.
    #[inline]
    pub fn to_publish_and_update_expiry_interval(&self) -> Option<Publish> {
        let mut publish = self.to_publish();

        if let Some(message_expiry_interval) = publish.properties.message_expiry_interval {
            let now = SystemTime::now();
            let expired_at = self.created_at + Duration::from_secs(message_expiry_interval as u64);
            match expired_at.duration_since(now) {
                Ok(duration) => {
                    publish.properties.message_expiry_interval = Some(duration.as_secs() as u32);
                }
                Err(_) => return None,
            }
        }

        Some(publish)
    }
}
