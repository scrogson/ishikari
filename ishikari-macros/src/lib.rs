use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, ImplItem, ItemImpl, LitInt, LitStr};

/// Implement [`Worker`] for a job type.
///
/// Every method written in the impl block is kept. Attributes set the common options without
/// writing the methods out:
///
/// - `queue = "name"`: the queue jobs of this type go to (`queue()`).
/// - `max_attempts = N`: how many times a job is tried before it is discarded
///   (`max_attempts()`).
/// - `timeout = "30s"`: how long one attempt may run (`timeout()`), in `ms`, `s`, `m` or `h`.
///
/// Setting an option both as an attribute and as a method is an error.
#[proc_macro_attribute]
pub fn worker(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut queue: Option<LitStr> = None;
    let mut max_attempts: Option<LitInt> = None;
    let mut timeout: Option<(LitStr, u64)> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("queue") {
            queue = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("max_attempts") {
            max_attempts = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("timeout") {
            let lit: LitStr = meta.value()?.parse()?;
            let millis = parse_duration_millis(&lit.value()).ok_or_else(|| {
                syn::Error::new(
                    lit.span(),
                    "expected a duration such as \"500ms\", \"30s\", \"5m\" or \"1h\"",
                )
            })?;
            timeout = Some((lit, millis));
            Ok(())
        } else {
            Err(meta.error("unknown worker option; expected `queue`, `max_attempts` or `timeout`"))
        }
    });
    parse_macro_input!(attr with parser);
    let mut input = parse_macro_input!(item as ItemImpl);

    let defines = |name: &str| {
        input
            .items
            .iter()
            .any(|item| matches!(item, ImplItem::Fn(f) if f.sig.ident == name))
    };
    let mut generated = Vec::new();
    if let Some(queue) = &queue {
        if defines("queue") {
            return syn::Error::new(
                queue.span(),
                "`queue` is set both here and as a method; keep one",
            )
            .to_compile_error()
            .into();
        }
        generated.push(syn::parse_quote! {
            fn queue(&self) -> &'static str {
                #queue
            }
        });
    }
    if let Some(max_attempts) = &max_attempts {
        if defines("max_attempts") {
            return syn::Error::new(
                max_attempts.span(),
                "`max_attempts` is set both here and as a method; keep one",
            )
            .to_compile_error()
            .into();
        }
        generated.push(syn::parse_quote! {
            fn max_attempts(&self) -> i32 {
                #max_attempts
            }
        });
    }
    if let Some((lit, millis)) = &timeout {
        if defines("timeout") {
            return syn::Error::new(
                lit.span(),
                "`timeout` is set both here and as a method; keep one",
            )
            .to_compile_error()
            .into();
        }
        generated.push(syn::parse_quote! {
            fn timeout(&self) -> ::std::option::Option<::std::time::Duration> {
                ::std::option::Option::Some(::std::time::Duration::from_millis(#millis))
            }
        });
    }
    input.items.extend(generated);

    quote! {
        #[async_trait::async_trait]
        #[typetag::serde]
        #input
    }
    .into()
}

/// `"500ms"`, `"30s"`, `"5m"` or `"1h"` in milliseconds.
fn parse_duration_millis(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().ok()?;
    let factor = match unit.trim() {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return None,
    };
    n.checked_mul(factor)
}

/// Derive what a job type needs: `Debug`, `Serialize` and `Deserialize` (through ishikari's
/// re-exported serde), alongside any other derives on the type.
#[proc_macro_attribute]
pub fn job(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);

    // Find and remove any existing derive attributes, collecting user derives
    let mut user_derives: Vec<syn::Path> = Vec::new();
    let mut errors: Vec<syn::Error> = Vec::new();

    input.attrs.retain(|attr| {
        if !attr.path().is_ident("derive") {
            return true;
        }
        let parsed = attr.parse_nested_meta(|meta| {
            match meta.path.segments.last().map(|s| s.ident.to_string()).as_deref() {
                Some("Serialize") | Some("Deserialize") => errors.push(syn::Error::new_spanned(
                    &meta.path,
                    "The #[ishikari::job] macro automatically derives serde traits. Please remove the manual serde derives.",
                )),
                Some("Debug") => {}
                _ => user_derives.push(meta.path.clone()),
            }
            Ok(())
        });
        if let Err(e) = parsed {
            errors.push(e);
        }
        false // Remove the original derive attribute
    });

    if let Some(mut first) = errors.pop() {
        for err in errors {
            first.combine(err);
        }
        return first.to_compile_error().into();
    }

    quote! {
        #[derive(Debug, #(#user_derives,)* ::serde::Serialize, ::serde::Deserialize)]
        #[serde(crate = "::ishikari::serde")]
        #input
    }
    .into()
}
