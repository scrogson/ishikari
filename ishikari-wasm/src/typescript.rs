//! A script's TypeScript, stripped to the JavaScript the runtime of scripts runs. Its types are
//! blanked out where they stand (swc's type stripper, which Node uses), so the JavaScript's
//! lines and columns are the TypeScript's, and so are those of what it throws. TypeScript that
//! isn't only types (an `enum`, a `namespace`, parameter properties) is transformed instead,
//! which moves code about. JavaScript is TypeScript without types: it comes out as it went in.

use std::sync::{Arc, Mutex};

use swc_common::errors::{DiagnosticBuilder, Emitter, Handler, HANDLER};
use swc_common::sync::Lrc;
use swc_common::{Globals, SourceMap, GLOBALS};
use swc_ts_fast_strip::{ErrorCode, Mode, Options};

/// The JavaScript of `source`, or why it isn't TypeScript.
pub(crate) fn javascript(source: &str) -> Result<String, String> {
    let javascript = match strip(source, Mode::StripOnly) {
        Err((ErrorCode::UnsupportedSyntax, _)) => strip(source, Mode::Transform),
        stripped => stripped,
    };
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

    #[test]
    fn an_enum_is_transformed() {
        let javascript = javascript("enum E { A = 'a' }\nexport const a = E.A;\n").unwrap();
        assert!(!javascript.contains("enum"), "{javascript}");
    }
}
