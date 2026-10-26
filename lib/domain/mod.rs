pub mod links;
pub mod posts;
pub mod schemas;
use crate::dal::Store;
#[derive(Clone)]
pub struct App {
    pub store: Store,
}
