pub mod admin;
pub mod content_jobs;
pub mod discovery;
pub mod editor_rules;
pub mod files;
pub mod links;
pub mod models;
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
pub mod training;
pub mod transcriptions;
pub mod transfers;
pub mod waitlist;
use crate::dal::Store;
#[derive(Clone)]
pub struct App {
    pub store: Store,
}
