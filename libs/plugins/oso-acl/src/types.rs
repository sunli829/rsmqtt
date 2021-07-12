use oso::PolarClass;
use service::RemoteAddr;

#[derive(Clone, PolarClass)]
pub struct Connection {
    pub addr: RemoteAddr,
    pub uid: Option<String>,
}
