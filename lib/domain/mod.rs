pub mod links;
pub mod posts;
pub mod rooms;
pub mod schemas;
pub mod transfers;
pub mod waitlist;
use crate::dal::Store;
#[derive(Clone)]
pub struct App {
    pub store: Store,
}
