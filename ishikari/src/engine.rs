mod postgres;
mod storage;

pub use postgres::Postgres;
pub use storage::Storage;

use crate::{queue::QueueBuilder, Stager, State};
use std::{marker::PhantomData, sync::Arc, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EngineName(Arc<str>);

impl From<&str> for EngineName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl EngineName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub struct EngineBuilder<S>
where
    S: Storage + 'static,
{
    name: EngineName,
    queues: Vec<QueueBuilder<S>>,
    stager_interval: Option<Duration>,
    stager_limit: Option<i32>,
    state: Option<State>,
    storage: PhantomData<S>,
}

impl<S> EngineBuilder<S>
where
    S: Storage + 'static,
{
    pub fn stager_interval(mut self, interval: Duration) -> Self {
        self.stager_interval = Some(interval);
        self
    }

    pub fn stager_limit(mut self, limit: i32) -> Self {
        self.stager_limit = Some(limit);
        self
    }

    pub fn with_queue(mut self, queue_builder: QueueBuilder<S>) -> Self {
        self.queues.push(queue_builder);
        self
    }

    pub fn with_state(mut self, state: State) -> Self {
        self.state = Some(state);
        self
    }

    pub fn start(self, storage: impl Into<Arc<S>>) -> Engine<S> {
        let storage = storage.into();
        let state = self.state.unwrap_or(Arc::new(()));

        let stager_interval = self.stager_interval.unwrap_or(Duration::from_secs(1));
        let stager_limit = self.stager_limit.unwrap_or(100);
        let stager = Stager::new(storage.clone(), stager_interval, stager_limit);
        let _stager_handle = stager.start();

        for queue in self.queues.into_iter() {
            let queue = queue.build(storage.clone(), state.clone());
            queue.start();
        }

        Engine {
            name: self.name,
            stager_interval,
            stager_limit,
            storage,
        }
    }
}

/// The Ishikari engine.
///
/// The engine is responsible for managing the queues and the stager.
#[allow(dead_code)]
pub struct Engine<S>
where
    S: Storage + 'static,
{
    name: EngineName,
    storage: Arc<S>,
    stager_interval: Duration,
    stager_limit: i32,
}

impl<S> Engine<S>
where
    S: Storage + 'static,
{
    pub fn builder(name: &str) -> EngineBuilder<S> {
        EngineBuilder {
            name: name.into(),
            queues: Vec::new(),
            stager_interval: None,
            stager_limit: None,
            state: None,
            storage: PhantomData,
        }
    }
}
