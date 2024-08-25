#[derive(Debug, Default)]
pub struct Complete(pub Option<String>);

#[derive(Debug, Default)]
pub struct Cancel(pub Option<String>);

#[derive(Debug)]
pub struct Snooze(pub u64);

pub enum Status {
    Complete(Complete),
    Cancel(Cancel),
    Snooze(Snooze),
}

pub type PerformError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type PerformResult = std::result::Result<Status, PerformError>;

impl From<Complete> for Status {
    fn from(s: Complete) -> Self {
        Self::Complete(s)
    }
}

impl From<Cancel> for Status {
    fn from(s: Cancel) -> Self {
        Self::Cancel(s)
    }
}

impl From<Snooze> for Status {
    fn from(s: Snooze) -> Self {
        Self::Snooze(s)
    }
}

impl Complete {
    pub fn message(mut self, message: impl ToString) -> Self {
        self.0 = Some(message.to_string());
        self
    }
}

impl Cancel {
    pub fn message(mut self, message: impl ToString) -> Self {
        self.0 = Some(message.to_string());
        self
    }
}

impl From<Complete> for PerformResult {
    fn from(complete: Complete) -> Self {
        Ok(Status::Complete(complete))
    }
}

impl From<Cancel> for PerformResult {
    fn from(cancel: Cancel) -> Self {
        Ok(Status::Cancel(cancel))
    }
}

impl From<Snooze> for PerformResult {
    fn from(snooze: Snooze) -> Self {
        Ok(Status::Snooze(snooze))
    }
}
