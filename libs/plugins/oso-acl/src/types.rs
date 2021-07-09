use oso::PolarClass;
use service::{RemoteAddr, TopicFilter};

#[derive(Clone, PolarClass)]
pub struct Filter(pub Option<TopicFilter>);

impl Filter {
    pub fn new(filter: String) -> Filter {
        Filter(TopicFilter::try_new(filter))
    }

    pub fn check(&self, other: Filter) -> bool {
        if let (Some(left), Some(right)) = (&self.0, &other.0) {
            left.is_overlapped(right)
        } else {
            false
        }
    }
}

#[derive(Clone, PolarClass)]
pub struct Connection {
    pub addr: RemoteAddr,
    pub uid: Option<String>,
}
