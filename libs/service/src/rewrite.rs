use std::borrow::Cow;

use anyhow::Result;
use regex::Regex;

use crate::config::RewriteConfig;

pub struct Rewrite {
    re: Regex,
    rep: String,
}

impl Rewrite {
    pub fn try_new(rewrite: &RewriteConfig) -> Result<Self> {
        Ok(Self {
            re: Regex::new(&rewrite.pattern)?,
            rep: rewrite.write.clone(),
        })
    }

    pub fn rewrite(&self, topic: &str) -> Option<String> {
        match self.re.replace(&*topic, &self.rep) {
            Cow::Borrowed(_) => None,
            Cow::Owned(new_topic) => Some(new_topic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_regex() {
        let rewrite = Rewrite::try_new(&RewriteConfig {
            pattern: "a/(.*)/c".to_string(),
            write: "k/$1/c".to_string(),
        })
        .unwrap();

        assert_eq!(rewrite.rewrite("a/1/c").unwrap(), "k/1/c");

        let rewrite = Rewrite::try_new(&RewriteConfig {
            pattern: "a/(.*)".to_string(),
            write: "k/$1".to_string(),
        })
        .unwrap();

        assert_eq!(rewrite.rewrite("a/1/c").unwrap(), "k/1/c");
        assert_eq!(rewrite.rewrite("a/c").unwrap(), "k/c");
        assert_eq!(rewrite.rewrite("a/c/1/2/3").unwrap(), "k/c/1/2/3");

        assert_eq!(rewrite.rewrite("d/c/1/2/3"), None);
    }
}
