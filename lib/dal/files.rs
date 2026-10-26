use crate::core::Settings;
#[derive(Clone)]
pub struct Files {
    pub settings: Option<Settings>,
    pub http: reqwest::Client,
}
impl Files {
    pub fn new(settings: Settings, http: reqwest::Client) -> Self {
        Self {
            settings: Some(settings),
            http,
        }
    }
    pub fn fixture() -> Self {
        Self {
            settings: None,
            http: reqwest::Client::new(),
        }
    }
}
