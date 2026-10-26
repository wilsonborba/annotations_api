pub mod links;
pub mod posts;
pub mod schemas;
pub mod waitlist;
use crate::dal::Store;
#[derive(Clone)]
pub struct App {
    pub store: Store,
}
