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

#[derive(Debug, Clone)]
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

    pub fn is_overlapped(&self, other: &Self) -> bool {
        for (left, right) in self.segments.iter().zip(other.segments.iter()) {
            match (left, right) {
                (Segment::Name(left_name), Segment::Name(right_name)) => {
                    if left_name != right_name {
                        return false;
                    }
                }
                (Segment::PlusSign, Segment::Name(_))
                | (Segment::Name(_), Segment::PlusSign)
                | (Segment::PlusSign, Segment::PlusSign) => {
                    return true;
                }
                (Segment::NumberSign, _) | (_, Segment::NumberSign) => {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! filter {
        ($path:expr) => {
            TopicFilter::try_new($path).unwrap()
        };
    }

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
        let filter = filter!("sport/tennis/player1/#");
        assert!(filter.has_wildcards());

        let filter = filter!("sport/tennis/+");
        assert!(filter.has_wildcards());

        let filter = filter!("sport/tennis/+/#");
        assert!(filter.has_wildcards());

        assert!(TopicFilter::try_new("sport/#/player1").is_none());

        let filter = filter!("$SYS/#");
        assert!(filter.has_wildcards());

        let filter = filter!("$SYS/tennis/player1");
        assert!(!filter.has_wildcards());

        let filter = filter!("$share/share1/tennis/player1");
        assert!(!filter.has_wildcards());
        assert_eq!(filter.share_name(), Some("share1"));
    }

    #[test]
    fn test_matches() {
        let filter = filter!("sport/tennis/player1/#");
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player1/ranking"));
        assert!(filter.matches("sport/tennis/player1/score/wimbledon"));

        let filter = filter!("sport/tennis/+");
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player2"));
        assert!(!filter.matches("sport/tennis/player1/ranking"));

        let filter = filter!("$share/share1/sport/tennis/+");
        assert!(filter.matches("sport/tennis/player1"));
        assert!(filter.matches("sport/tennis/player2"));
        assert!(!filter.matches("sport/tennis/player1/ranking"));

        let filter = filter!("sport/+");
        assert!(!filter.matches("sport"));
        assert!(filter.matches("sport/"));

        let filter = filter!("+/monitor/Clients");
        assert!(!filter.matches("$SYS/monitor/Clients"));

        let filter = filter!("$SYS/#");
        assert!(filter.matches("$SYS/monitor/Clients"));

        let filter = filter!("$SYS/monitor/+");
        assert!(filter.matches("$SYS/monitor/Clients"));

        let filter = filter!("#");
        assert!(!filter.matches("$SYS/monitor/Clients"));
    }

    #[test]
    fn test_is_overlapped() {
        assert!(filter!("a/b").is_overlapped(&filter!("a/+")));
        assert!(!filter!("a").is_overlapped(&filter!("a/+")));
        assert!(filter!("a/+").is_overlapped(&filter!("a/b")));

        assert!(filter!("a/b/c").is_overlapped(&filter!("a/+/c")));
        assert!(filter!("a/+/c").is_overlapped(&filter!("a/b/c")));
        assert!(!filter!("a/b/c/+").is_overlapped(&filter!("a/b/c")));
        assert!(!filter!("a/b/c").is_overlapped(&filter!("a/b/c/+")));

        assert!(filter!("a/#").is_overlapped(&filter!("a/b")));
        assert!(filter!("a/#").is_overlapped(&filter!("a/b")));
        assert!(filter!("a/b").is_overlapped(&filter!("a/#")));
        assert!(filter!("a/b/#").is_overlapped(&filter!("a/b/c")));
        assert!(!filter!("a/b/#").is_overlapped(&filter!("a/b")));

        assert!(filter!("a/b/#").is_overlapped(&filter!("a/b/+/d")));

        assert!(filter!("#").is_overlapped(&filter!("a/b")));
        assert!(filter!("#").is_overlapped(&filter!("#")));

        assert!(filter!("#").is_overlapped(&filter!("#")));
        assert!(filter!("#").is_overlapped(&filter!("a/#")));
        assert!(!filter!("a/b/c/#").is_overlapped(&filter!("a/b/d/#")));
    }
}
