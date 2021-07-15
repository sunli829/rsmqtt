#[inline]
pub fn valid_topic(topic: &str) -> bool {
    if topic.is_empty() {
        return false;
    }
    !topic.contains(&['+', '#'][..])
}

#[inline]
pub fn has_wildcards(filter: &str) -> bool {
    filter.contains(&['+', '#'][..])
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Filter<'a> {
    pub share_name: Option<&'a str>,
    pub path: &'a str,
}

#[inline]
fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    for segment in filter.split('/') {
        if segment.contains(&['+', '#'][..]) && segment.len() != 1 {
            return false;
        }
    }

    true
}

#[inline]
pub fn parse_filter(filter: &str) -> Option<Filter> {
    if let Some(mut tail) = filter.strip_prefix("$share") {
        if !tail.starts_with('/') {
            return None;
        }
        tail = &tail[1..];
        if tail.is_empty() {
            return None;
        }
        let (share_name, path) = tail.split_once('/')?;
        if has_wildcards(share_name) {
            return None;
        }
        if !valid_filter(path) {
            return None;
        }
        Some(Filter {
            share_name: Some(share_name),
            path,
        })
    } else {
        if !valid_filter(filter) {
            return None;
        }
        Some(Filter {
            share_name: None,
            path: filter,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            parse_filter("abc/a/b"),
            Some(Filter {
                share_name: None,
                path: "abc/a/b"
            })
        );

        assert_eq!(
            parse_filter("$share/abc/a/b"),
            Some(Filter {
                share_name: Some("abc"),
                path: "a/b"
            })
        );

        assert_eq!(parse_filter("$share"), None);
        assert_eq!(parse_filter("$share/"), None);
        assert_eq!(parse_filter("$share/abc/a/b#/c"), None);
        assert_eq!(parse_filter("$share/abc/a/b+/c"), None);

        assert_eq!(
            parse_filter("$share/abc/a/+/c"),
            Some(Filter {
                share_name: Some("abc"),
                path: "a/+/c"
            })
        );
        assert_eq!(
            parse_filter("$share/abc/a/#"),
            Some(Filter {
                share_name: Some("abc"),
                path: "a/#"
            })
        );
    }
}
