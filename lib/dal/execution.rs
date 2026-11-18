//! Cloud execution adapter for transcription runs. No server runtime exists yet, so the
//! only implementation reports every release as unavailable and never produces a result.
use std::sync::Arc;

/// A server runtime that can execute transcription runs of a model release.
pub trait CloudExecution: Send + Sync {
    /// True when a server runtime can run this model release.
    fn supports(&self, model_version: &str) -> bool;
}

/// The default adapter: no model release has a server runtime.
pub struct NoCloudRuntime;
impl CloudExecution for NoCloudRuntime {
    fn supports(&self, _model_version: &str) -> bool {
        false
    }
}

pub fn none() -> Arc<dyn CloudExecution> {
    Arc::new(NoCloudRuntime)
}
