use std::sync::Arc;

use service::ServiceState;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

pub fn metrics(
    state: Arc<ServiceState>,
) -> impl Filter<Extract = (Response,), Error = Rejection> + Clone {
    warp::path!("metrics")
        .and(warp::any().map(move || state.clone()))
        .map(|state: Arc<ServiceState>| {
            let metrics = state.metrics();
            warp::reply::json(&metrics).into_response()
        })
}
