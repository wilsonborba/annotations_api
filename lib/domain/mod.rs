pub mod discovery;
pub mod editor_rules;
pub mod files;
pub mod links;
pub mod names;
pub mod note_documents;
pub mod note_saves;
pub mod notes;
pub mod posts;
pub mod projects;
pub mod publication;
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
