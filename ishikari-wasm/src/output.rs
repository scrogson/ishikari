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
pub(crate) struct Output(Arc<Mutex<Kept>>);

#[derive(Default)]
struct Kept {
    bytes: Vec<u8>,
    /// Whether some was dropped.
    cut: bool,
}

impl Output {
    /// What was written, without its last line when that was cut short (it may end inside a
    /// secret's value, which then can't be redacted).
    pub fn text(&self) -> String {
        let kept = self.0.lock().unwrap();
        let text = String::from_utf8_lossy(&kept.bytes);
        match text.rfind('\n') {
            Some(end) if kept.cut => text[..end].to_string(),
            None if kept.cut => String::new(),
            _ => text.into_owned(),
        }
    }

    fn keep(&self, bytes: &[u8]) {
        let mut kept = self.0.lock().unwrap();
        let room = CAPACITY.saturating_sub(kept.bytes.len());
        kept.bytes
            .extend_from_slice(&bytes[..bytes.len().min(room)]);
        kept.cut |= bytes.len() > room;
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
