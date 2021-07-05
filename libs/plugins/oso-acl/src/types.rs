use oso::PolarClass;
use service::{RemoteAddr, TopicFilter};

#[derive(Clone, PolarClass)]
pub struct Filter(Option<TopicFilter>);

impl Filter {
    pub fn new(filter: String) -> Filter {
        Filter(TopicFilter::try_new(&filter))
    }

    pub fn test(&self, topic: String) -> bool {
        match &self.0 {
            Some(filter) => filter.matches(&topic),
            None => false,
        }
    }
}

#[derive(Clone, PolarClass)]
pub struct Connection {
    pub addr: RemoteAddr,
    pub uid: Option<String>,
}
