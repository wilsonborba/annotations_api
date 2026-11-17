pub mod editor_rules;
pub mod links;
pub mod names;
pub mod note_documents;
pub mod posts;
pub mod rooms;
pub mod schemas;
pub mod tasks;
pub mod transfers;
pub mod waitlist;
use crate::dal::Store;
#[derive(Clone)]
pub struct App {
    pub store: Store,
}
