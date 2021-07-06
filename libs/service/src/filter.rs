use std::collections::HashMap;
use std::num::NonZeroUsize;

use bytestring::ByteString;
use codec::{Qos, RetainHandling};
use serde::{Deserialize, Serialize};

use crate::Message;

#[inline]
pub fn valid_topic(topic: &str) -> bool {
    if topic.is_empty() {
        return false;
    }
    !topic.contains(&['+', '#'][..])
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
enum Segment {
    Name(ByteString),
    NumberSign,
    PlusSign,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFilter {
    path: ByteString,
    has_wildcards: bool,
    share_name: Option<ByteString>,
    segments: Vec<Segment>,
}

impl TopicFilter {
    pub fn try_new(filter: impl Into<ByteString>) -> Option<TopicFilter> {
        let filter = filter.into();
        let mut segments = Vec::new();
        let mut is_share = false;
        let mut share_name = None;
        let mut number_sign = false;
        let mut has_wildcards = false;

        for (idx, s) in filter.split('/').enumerate() {
            if s == "$share" && idx == 0 {
                is_share = true;
                continue;
            }

            if is_share && idx == 1 {
                share_name = Some(s.to_string().into());
                continue;
            }

            if number_sign {
                return None;
            }

            match s {
                "#" => {
                    segments.push(Segment::NumberSign);
                    number_sign = true;
                    has_wildcards = true;
                }
                "+" => {
                    segments.push(Segment::PlusSign);
                    has_wildcards = true;
                }
                _ => segments.push(Segment::Name(s.to_string().into())),
            }
        }

        if is_share && share_name.is_none() {
            return None;
        }

        Some(TopicFilter {
            path: filter,
            has_wildcards,
            share_name,
            segments,
        })
    }

    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[inline]
    pub fn has_wildcards(&self) -> bool {
        self.has_wildcards
    }

    #[inline]
    pub fn is_share(&self) -> bool {
        self.share_name.is_some()
    }

    #[inline]
    pub fn share_name(&self) -> Option<&str> {
        self.share_name.as_deref()
    }

    pub fn matches(&self, topic: &str) -> bool {
        if topic.is_empty() {
            return false;
        }

        let mut topics = topic.split('/');

        for segment in &self.segments {
            match (topics.next(), segment) {
                (None, Segment::NumberSign) => return true,
                (Some(t), Segment::NumberSign) if !t.starts_with('$') => return true,
                (Some(t), Segment::PlusSign) if !t.starts_with('$') => continue,
                (Some(t), Segment::Name(s)) if t == s.as_ref() as &str => continue,
                _ => return false,
            }
        }

        if topics.next().is_some() {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterItem {
    pub topic_filter: TopicFilter,
    pub qos: Qos,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
    pub id: Option<NonZeroUsize>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Filters(HashMap<String, FilterItem>);

impl Filters {
    #[inline]
    pub fn insert(&mut self, item: FilterItem) -> Option<FilterItem> {
        self.0.insert(item.topic_filter.path.to_string(), item)
    }

    #[inline]
    pub fn remove(&mut self, path: &str) -> Option<FilterItem> {
        self.0.remove(path)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn filter_message(&self, client_id: &str, msg: &Message) -> Option<Message> {
        let mut matched = false;
        let mut max_qos = Qos::AtMostOnce;
        let mut retain = msg.is_retain();
        let mut ids = Vec::new();

        if msg.is_expired() {
            return None;
        }

        for filter in self.0.values() {
            if filter.no_local && msg.publisher().map(|s| &**s) == Some(client_id) {
                // If no local is true, Application Messages MUST NOT be forwarded to a connection with
                // a ClientID equal to the ClientID of the publishing connection [MQTT-3.8.3-3]
                continue;
            }

            if !filter.topic_filter.matches(msg.topic()) {
                continue;
            }

            if let Some(id) = filter.id {
                // If the Client specified a Subscription Identifier for any of the overlapping
                // subscriptions the Server MUST send those Subscription Identifiers in the message
                // which is published as the result of the subscriptions [MQTT-3.3.4-3].
                //
                // If the Server sends a single copy of the message it MUST include in the PUBLISH packet
                // the Subscription Identifiers for all matching subscriptions which have a Subscription Identifiers,
                // their order is not significant [MQTT-3.3.4-4].
                ids.push(id);
            }

            // When Clients make subscriptions with Topic Filters that include wildcards, it is possible
            // for a Client’s subscriptions to overlap so that a published message might match multiple filters.
            // In this case the Server MUST deliver the message to the Client respecting the maximum QoS of all
            // the matching subscriptions [MQTT-3.3.4-2].
            max_qos = max_qos.max(filter.qos);

            if !filter.retain_as_published {
                retain = false;
            }

            matched = true;
        }

        if matched {
            let mut properties = msg.properties().clone();
            properties.subscription_identifiers = ids;
            let msg = Message::new(
                msg.topic().clone(),
                msg.qos().min(max_qos),
                msg.payload().clone(),
            )
            .with_retain(retain)
            .with_properties(properties);
            Some(msg)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_topic() {
        assert!(!valid_topic(""));
        assert!(valid_topic("sport/tennis/player1"));
        assert!(!valid_topic("sport/tennis+/player1"));
        assert!(!valid_topic("sport/tennis/+/player1"));
        assert!(!valid_topic("sport/tennis#/player1"));
        assert!(!valid_topic("sport/tennis/#/player1"));
    }

    #[test]
    fn test_new() {
        let filter = TopicFilter::try_new("sport/tennis/player1/#").unwrap();
        assert!(filter.has_wildcards());

        let filter = TopicFilter::try_new("sport/tennis/+").unwrap();
        assert!(filter.has_wildcards());

        let filter = TopicFilter::try_new("sport/tennis/+/#").unwrap();
        assert!(filter.has_wildcards());

        assert!(TopicFilter::try_new("sport/#/player1").is_none());

        let filter = TopicFilter::try_new("$SYS/#").unwrap();
        assert!(filter.has_wildcards());

        let filter = TopicFilter::try_new("$SYS/tennis/player1").unwrap();
        assert!(!filter.has_wildcards());

        let filter = TopicFilter::try_new("$share/share1/tennis/player1").unwrap();
        assert!(!filter.has_wildcards());
        assert_eq!(filter.share_name(), Some("share1"));
    }

    #[test]
    fn test_matches() {
        let filter = TopicFilter::try_new("sport/tennis/player1/#").unwrap();
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player1/ranking"));
        assert!(filter.matches("sport/tennis/player1/score/wimbledon"));

        let filter = TopicFilter::try_new("sport/tennis/+").unwrap();
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player2"));
        assert!(!filter.matches("sport/tennis/player1/ranking"));

        let filter = TopicFilter::try_new("$share/share1/sport/tennis/+").unwrap();
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player2"));
        assert!(!filter.matches("sport/tennis/player1/ranking"));

        let filter = TopicFilter::try_new("sport/+").unwrap();
        assert!(!filter.matches("sport"));
        assert!(filter.matches("sport/"));

        let filter = TopicFilter::try_new("+/monitor/Clients").unwrap();
        assert!(!filter.matches("$SYS/monitor/Clients"));

        let filter = TopicFilter::try_new("$SYS/#").unwrap();
        assert!(filter.matches("$SYS/monitor/Clients"));

        let filter = TopicFilter::try_new("$SYS/monitor/+").unwrap();
        assert!(filter.matches("$SYS/monitor/Clients"));

        let filter = TopicFilter::try_new("#").unwrap();
        assert!(!filter.matches("$SYS/monitor/Clients"));
    }
}
