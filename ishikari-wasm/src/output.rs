//! A guest's stdout or stderr, kept for its node's logs.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::AsyncWrite;
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::{OutputStream, Pollable, StreamError};

/// How much of a stream is kept; the rest is dropped, so a chatty guest isn't failed for it.
const CAPACITY: usize = 64 * 1024;

/// What a guest wrote to a stream, up to [`CAPACITY`] bytes.
#[derive(Clone, Default)]
pub(crate) struct Output(Arc<Mutex<Vec<u8>>>);

impl Output {
    /// What was written, by line.
    pub fn lines(&self) -> impl Iterator<Item = String> {
        let written = String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned();
        written
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn keep(&self, bytes: &[u8]) {
        let mut kept = self.0.lock().unwrap();
        let room = CAPACITY.saturating_sub(kept.len());
        kept.extend_from_slice(&bytes[..bytes.len().min(room)]);
    }
}

impl IsTerminal for Output {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for Output {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }

    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

#[wasmtime_wasi::async_trait]
impl OutputStream for Output {
    fn write(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        self.keep(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StreamError> {
        Ok(())
    }

    fn check_write(&mut self) -> Result<usize, StreamError> {
        Ok(CAPACITY)
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for Output {
    async fn ready(&mut self) {}
}

impl AsyncWrite for Output {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.keep(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
