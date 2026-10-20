//! The runtime of script modules: node types written in TypeScript or JavaScript. A component
//! of the `script-node` world (`../wit/node.wit`) that evaluates the script `script.source`
//! gives it with Boa, a JavaScript engine in Rust, and calls the script's `spec` or `run`.
//! ishikari-wasm strips a script's TypeScript before it gets here.
//!
//! A script is an ES module exporting `spec`, the node type as an object, and `run`, a function
//! (or an async one) from the node's input to its output. It can't import other modules. Beyond
//! the language it has boa_runtime's web APIs: `console` (the node's logs), `fetch` (through
//! `host.fetch`), `URL`, `TextEncoder` and `TextDecoder`, `atob` and `btoa`, `structuredClone`,
//! `queueMicrotask`, `setTimeout` and `setInterval`, and `AbortController`.

mod executor;

use std::cell::RefCell;
use std::future::{self, Future};
use std::path::Path;
use std::rc::Rc;

use boa_engine::builtins::promise::PromiseState;
use boa_engine::context::ContextBuilder;
use boa_engine::{
    js_error, js_string, Context, Finalize, JsData, JsError, JsObject, JsResult, JsString, JsValue,
    Module, Source, Trace,
};
use boa_runtime::extensions::{ConsoleExtension, FetchExtension};
use boa_runtime::fetch::request::JsRequest;
use boa_runtime::fetch::response::JsResponse;
use boa_runtime::fetch::Fetcher;
use boa_runtime::{ConsoleState, Logger};

wit_bindgen::generate!({ path: "../wit", world: "script-node" });

use ishikari::node::host::{self, Level, Request};
use ishikari::node::script;

struct Runtime;

impl Guest for Runtime {
    fn spec() -> Result<String, String> {
        let mut script = Script::load()?;
        let spec = script.export("spec")?;
        if spec.is_undefined() {
            return Err("the script exports no `spec`".into());
        }
        script.json(spec)
    }

    fn run(input: String) -> Result<String, String> {
        let mut script = Script::load()?;
        let Some(run) = script.export("run")?.as_callable() else {
            return Err("the script exports no `run` function".into());
        };
        let input: serde_json::Value =
            serde_json::from_str(&input).map_err(|e| format!("its input isn't JSON: {e}"))?;
        let input = JsValue::from_json(&input, &mut script.context);
        let output =
            input.and_then(|input| run.call(&JsValue::undefined(), &[input], &mut script.context));
        let output = script.settled(output)?;
        script.json(output)
    }
}

export!(Runtime);

/// A script evaluated in a context of its own.
struct Script {
    context: Context,
    module: Module,
}

impl Script {
    /// The script `script.source` gives, evaluated.
    fn load() -> Result<Self, String> {
        let mut context = ContextBuilder::new()
            .job_executor(Rc::new(executor::Executor::default()))
            .build()
            .map_err(|e| e.to_string())?;
        boa_runtime::register(
            (ConsoleExtension(HostLogger), FetchExtension(HostFetcher)),
            None,
            &mut context,
        )
        .map_err(|e| e.to_string())?;

        let source = script::source();
        let source = Source::from_bytes(source.as_bytes()).with_path(Path::new("script"));
        let module = Module::parse(source, None, &mut context);
        let module = module.map_err(|e| native(e.to_string()))?;
        let mut script = Script { context, module };
        let evaluated = script.module.load_link_evaluate(&mut script.context);
        script.settled(Ok(evaluated.into()))?;
        Ok(script)
    }

    /// What the script exports as `name`.
    fn export(&mut self, name: &str) -> Result<JsValue, String> {
        let exports = self.module.namespace(&mut self.context);
        let export = exports.get(JsString::from(name), &mut self.context);
        export.map_err(|e| self.message(e))
    }

    /// `value`, or what it settles to when it's a promise, once every job it waits on has run.
    fn settled(&mut self, value: JsResult<JsValue>) -> Result<JsValue, String> {
        let value = value.map_err(|e| self.message(e))?;
        let Some(promise) = value.as_promise() else {
            return Ok(value);
        };
        self.context.run_jobs().map_err(|e| self.message(e))?;
        match promise.state() {
            PromiseState::Fulfilled(value) => Ok(value),
            PromiseState::Rejected(reason) => Err(self.message(JsError::from_opaque(reason))),
            PromiseState::Pending => Err("its promise never settled".into()),
        }
    }

    /// `value` as JSON, by `JSON.stringify`; `null` for what has none (`undefined`).
    fn json(&mut self, value: JsValue) -> Result<String, String> {
        let json = self.stringify(value).map_err(|e| self.message(e))?;
        Ok(json.unwrap_or_else(|| "null".into()))
    }

    fn stringify(&mut self, value: JsValue) -> JsResult<Option<String>> {
        let context = &mut self.context;
        let json = context.global_object().get(js_string!("JSON"), context)?;
        let stringify = json
            .as_object()
            .map(|json| json.get(js_string!("stringify"), context))
            .transpose()?
            .and_then(|stringify| stringify.as_callable())
            .ok_or_else(|| js_error!(TypeError: "JSON.stringify is gone"))?;
        let json = stringify.call(&json, &[value], context)?;
        if json.is_undefined() {
            return Ok(None);
        }
        Ok(Some(json.to_string(context)?.to_std_string_escaped()))
    }

    /// What `error` says: `TypeError: ... (script:7:19)` for an error, with where it was
    /// thrown when that's known; else what was thrown, as JSON.
    fn message(&mut self, error: JsError) -> String {
        if let Ok(error) = error.try_native(&mut self.context) {
            return native(error.to_string());
        }
        let thrown = match error.into_opaque(&mut self.context) {
            Ok(thrown) => thrown,
            Err(error) => return error.to_string(),
        };
        if let Some(string) = thrown.as_string() {
            return string.to_std_string_escaped();
        }
        match self.stringify(thrown.clone()) {
            Ok(Some(json)) => json,
            _ => thrown.display().to_string(),
        }
    }
}

/// A native error's message, without the ` (native)` Boa ends one with when it has no position.
fn native(message: String) -> String {
    match message.strip_suffix(" (native)") {
        Some(message) => message.to_string(),
        None => message,
    }
}

/// `console`, into the node's logs.
#[derive(Debug, Trace, Finalize)]
struct HostLogger;

impl Logger for HostLogger {
    fn debug(&self, msg: String, _: &ConsoleState, _: &mut Context) -> JsResult<()> {
        host::log(Level::Debug, &msg);
        Ok(())
    }

    fn log(&self, msg: String, _: &ConsoleState, _: &mut Context) -> JsResult<()> {
        host::log(Level::Info, &msg);
        Ok(())
    }

    fn info(&self, msg: String, _: &ConsoleState, _: &mut Context) -> JsResult<()> {
        host::log(Level::Info, &msg);
        Ok(())
    }

    fn warn(&self, msg: String, _: &ConsoleState, _: &mut Context) -> JsResult<()> {
        host::log(Level::Warn, &msg);
        Ok(())
    }

    fn error(&self, msg: String, _: &ConsoleState, _: &mut Context) -> JsResult<()> {
        host::log(Level::Error, &msg);
        Ok(())
    }
}

/// `fetch`, through `host.fetch`: only to the hosts the module was granted.
#[derive(Debug, Clone, Trace, Finalize, JsData)]
struct HostFetcher;

impl Fetcher for HostFetcher {
    fn fetch(
        self: Rc<Self>,
        request: JsRequest,
        _signal: Option<JsObject>,
        _context: &RefCell<&mut Context>,
    ) -> impl Future<Output = JsResult<JsResponse>> {
        future::ready(fetch(request))
    }
}

fn fetch(request: JsRequest) -> JsResult<JsResponse> {
    let request = request.into_inner();
    let url = request.uri().to_string();
    let headers = request
        .headers()
        .iter()
        .map(|(name, value)| {
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            (name.to_string(), value)
        })
        .collect();
    let body = Some(request.body().clone()).filter(|body| !body.is_empty());
    let response = host::fetch(&Request {
        method: request.method().to_string(),
        url: url.clone(),
        headers,
        body,
    })
    .map_err(|e| js_error!(TypeError: "fetch {} failed: {}", url, e))?;

    let mut builder = http::Response::builder().status(response.status);
    for (name, value) in &response.headers {
        builder = builder.header(name, value);
    }
    let response = builder.body(response.body).map_err(JsError::from_rust)?;
    Ok(JsResponse::basic(JsString::from(url.as_str()), response))
}
