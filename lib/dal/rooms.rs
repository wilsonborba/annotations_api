#[derive(Clone)]
pub struct Rooms {
    pub redis: Option<redis::Client>,
}
impl Rooms {
    pub fn new(redis: Option<redis::Client>) -> Self {
        Self { redis }
    }
    pub fn fixture() -> Self {
        Self::new(None)
    }
}
