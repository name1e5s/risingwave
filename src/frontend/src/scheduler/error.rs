use risingwave_common::array::ArrayError;
use risingwave_common::error::{ErrorCode, RwError, TrackingIssue};
use thiserror::Error;

use crate::scheduler::plan_fragmenter::QueryId;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Array error: {0}")]
    Array(ArrayError),

    #[error("Create task error: {0}")]
    CreateTask(RwError),

    #[error("Pin snapshot error: {0} fails to get epoch {1}")]
    PinSnapshot(QueryId, u64),

    #[error("Tonic Status: {0}")]
    TonicStatus(tonic::Status),

    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<SchedulerError> for RwError {
    fn from(s: SchedulerError) -> Self {
        ErrorCode::SchedulerError(Box::new(s)).into()
    }
}

impl From<tonic::Status> for SchedulerError {
    fn from(s: tonic::Status) -> Self {
        SchedulerError::TonicStatus(s)
    }
}
