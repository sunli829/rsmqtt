use std::sync::Arc;

use serde::Serialize;
use warp::reply::Response;
use warp::{Filter, Rejection, Reply};

use crate::server::ServerState;

pub fn stat(
    state: Arc<ServerState>,
) -> impl Filter<Extract = (Response,), Error = Rejection> + Clone {
    warp::path!("stat")
        .and(warp::any().map(move || state.clone()))
        .map(|state: Arc<ServerState>| {
            #[derive(Serialize)]
            struct Item<'a> {
                name: &'a str,
                value: &'a str,
            }

            let mut data = Vec::new();
            let stat_items = state.stat_receiver.borrow();
            for (name, value) in &*stat_items {
                data.push(Item { name, value })
            }
            data.sort_by(|a, b| a.name.cmp(b.name));
            warp::reply::json(&data).into_response()
        })
}
