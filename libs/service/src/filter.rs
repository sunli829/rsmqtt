use bytestring::ByteString;

#[inline]
pub fn valid_topic(topic: &str) -> bool {
    if topic.is_empty() {
        return false;
    }
    !topic.contains(&['+', '#'][..])
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum Segment {
    Name(ByteString),
    NumberSign,
    PlusSign,
}

#[derive(Clone)]
pub struct TopicFilter {
    has_wildcards: bool,
    share_name: Option<ByteString>,
    segments: Vec<Segment>,
}

impl TopicFilter {
    pub fn try_new(filter: &str) -> Option<TopicFilter> {
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
            has_wildcards,
            share_name,
            segments,
        })
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
