//! A script's TypeScript, stripped to the JavaScript the runtime of scripts runs. Its types are
//! blanked out where they stand (swc's type stripper, which Node uses), so the JavaScript's
//! lines and columns are the TypeScript's, and so are those of what it throws. TypeScript that
//! isn't only types (an `enum`, a `namespace`, parameter properties) is transformed instead,
//! which moves code about. JavaScript is TypeScript without types: it comes out as it went in.
//!
//! swc runs natively, on a thread of its own with a stack deep enough for any script that
//! [`nesting`](crate::nesting) lets through.

use std::sync::{Arc, Mutex};

use swc_common::errors::{DiagnosticBuilder, Emitter, Handler, HANDLER};
use swc_common::sync::Lrc;
use swc_common::{Globals, SourceMap, GLOBALS};
use swc_ts_fast_strip::{ErrorCode, Mode, Options};

/// The stack swc strips a script on. The deepest script it's given takes under 2 MiB in a release
/// build, and under 16 in a debug one.
const STACK_BYTES: usize = 64 * 1024 * 1024;

/// The JavaScript of `source`, or why it isn't TypeScript.
pub(crate) fn javascript(source: &str) -> Result<String, String> {
    crate::nesting::check(source)?;
    let source = source.to_string();
    let failed = |e: &dyn std::fmt::Display| format!("stripping the script's types failed: {e}");
    let javascript = std::thread::Builder::new()
        .name("ishikari-typescript".into())
        .stack_size(STACK_BYTES)
        .spawn(move || match strip(&source, Mode::StripOnly) {
            Err((ErrorCode::UnsupportedSyntax, _)) => strip(&source, Mode::Transform),
            stripped => stripped,
        })
        .map_err(|e| failed(&e))?
        .join()
        .map_err(|_| failed(&"it panicked"))?;
    javascript.map_err(|(_, why)| format!("the script isn't valid TypeScript: {why}"))
}

fn strip(source: &str, mode: Mode) -> Result<String, (ErrorCode, String)> {
    let source_map: Lrc<SourceMap> = Default::default();
    let errors = Errors {
        source_map: source_map.clone(),
        errors: Arc::default(),
    };
    let reported = errors.errors.clone();
    let handler = Handler::with_emitter(true, false, Box::new(errors));
    let options = Options {
        module: Some(true),
        filename: Some("script".into()),
        mode,
        ..Options::default()
    };
    // Some of what swc reports (an `enum`, in strip-only mode) goes to the `HANDLER` it's given.
    let stripped = GLOBALS.set(&Globals::new(), || {
        HANDLER.set(&handler, || {
            swc_ts_fast_strip::operate(&source_map, &handler, source.to_string(), options)
        })
    });
    stripped.map(|output| output.code).map_err(|error| {
        let reported = reported.lock().unwrap();
        let why = match reported.is_empty() {
            true => error.message,
            false => reported.join("; "),
        };
        (error.code, why)
    })
}

/// The errors swc reports, as `what (script:line:column)`.
struct Errors {
    source_map: Lrc<SourceMap>,
    errors: Arc<Mutex<Vec<String>>>,
}

impl Emitter for Errors {
    fn emit(&mut self, diagnostic: &mut DiagnosticBuilder<'_>) {
        let mut error = diagnostic.message();
        if let Some(span) = diagnostic.span.primary_span() {
            let at = self.source_map.lookup_char_pos(span.lo);
            error = format!("{error} (script:{}:{})", at.line, at.col.0 + 1);
        }
        self.errors.lock().unwrap().push(error);
    }
}

#[cfg(test)]
mod tests {
    use super::javascript;

    #[test]
    fn types_are_blanked_where_they_stand() {
        let typescript = "const n: number = f<string>(1 as any);\nexport { n };\n";
        let javascript = javascript(typescript).unwrap();
        assert_eq!(javascript.lines().count(), typescript.lines().count());
        assert!(javascript.starts_with("const n         = f        (1       );"));
    }

    #[test]
    fn javascript_comes_out_as_it_went_in() {
        let source = "export const spec = { name: 'x' };\nexport function run() {}\n";
        assert_eq!(javascript(source).unwrap(), source);
    }

    /// The stack of a tokio blocking thread, which a script used to be stripped on.
    const BLOCKING_STACK: usize = 2 * 1024 * 1024;

    #[test]
    fn a_deep_script_is_stripped_on_a_stack_of_its_own_and_a_deeper_one_refused() {
        let nested = |depth| {
            format!(
                "export const x = {}1{};",
                "(".repeat(depth),
                ")".repeat(depth)
            )
        };
        let (deep, deeper) = std::thread::Builder::new()
            .stack_size(BLOCKING_STACK)
            .spawn(move || (javascript(&nested(700)), javascript(&nested(100_000))))
            .unwrap()
            .join()
            .unwrap();
        deep.unwrap();
        let error = deeper.unwrap_err();
        assert!(error.contains("nests more than"), "{error}");
    }

    /// Each way of nesting, `depth` deep.
    fn nested(depth: usize) -> Vec<String> {
        let wrap = |open: &str, inner: &str, close: &str| {
            format!("{}{inner}{}", open.repeat(depth), close.repeat(depth))
        };
        vec![
            wrap("x = (", "1", ")"),
            wrap("x = [", "1", "]"),
            wrap("x = {a: ", "1", "}"),
            wrap("{", "", "}"),
            wrap("x = `${", "1", "}`"),
            wrap("type T = A<", "1", ">"),
            wrap("x = a => ", "1", ""),
            wrap("x = async (a: number): Promise<number> => ", "1", ""),
            wrap("x = function () { return ", "1", " }"),
            wrap("x = class { m() { return ", "1", " } }"),
            wrap("x = class extends ", "Object", " {}"),
            wrap("x = ({ a: ", "b", " }) => 1"),
            wrap("type F = (a: ", "1", ") => void"),
            wrap("type C = A extends B ? ", "1", " : 2"),
            wrap("x = !", "1", ""),
            wrap("x = a ? 1 : ", "1", ""),
            wrap("x = a = ", "1", ""),
            wrap("x = 2 ** ", "1", ""),
            wrap("x = 1 + ", "1", ""),
            wrap("x = new ", "A", "()"),
            wrap("x = typeof ", "1", ""),
            wrap("x = await ", "1", ""),
            wrap("x = ", "a", ".b"),
            wrap("x = ", "f", "(1)"),
            wrap("x = ", "a", " as A"),
            wrap("if (a) ", "x", ""),
            wrap("while (a) ", "x", ""),
            wrap("do ", "x", "; while (a)"),
            wrap("if (a) {} else ", "{}", ""),
            format!("if (a) {{}}{}", " else if (a) {}".repeat(depth)),
        ]
    }

    #[test]
    fn a_script_nested_as_deeply_as_it_may_be_is_stripped() {
        for (way, deepest) in (0..nested(1).len()).map(|way| {
            // The depth the check lets through, found by halving.
            let (mut low, mut high) = (0, crate::nesting::MAX_DEPTH);
            while low < high {
                let depth = (low + high).div_ceil(2);
                match crate::nesting::check(&nested(depth)[way]) {
                    Ok(()) => low = depth,
                    Err(_) => high = depth - 1,
                }
            }
            (way, low)
        }) {
            let source = &nested(deepest)[way];
            assert!(deepest > 0, "{source}");
            // An overflow aborts the test's process.
            let _ = javascript(source);
        }
    }

    #[test]
    fn an_enum_is_transformed() {
        let javascript = javascript("enum E { A = 'a' }\nexport const a = E.A;\n").unwrap();
        assert!(!javascript.contains("enum"), "{javascript}");
    }
}
