use proc_macro::TokenStream;
use quote::*;
use syn::{
    parse_macro_input, AttributeArgs, DeriveInput, Ident, ItemImpl, Lit, Meta, MetaNameValue,
    NestedMeta,
};

#[proc_macro_attribute]
pub fn worker(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as AttributeArgs);
    let input = parse_macro_input!(item as ItemImpl);

    // Get the type being implemented
    let ty = &input.self_ty;

    // Default values
    let mut queue = quote! { "default" };
    let mut max_attempts = quote! { 20 };

    // Parse attribute arguments
    for arg in args {
        if let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. })) = arg {
            let ident = path.get_ident().unwrap().to_string();
            match ident.as_str() {
                "queue" => {
                    if let Lit::Str(s) = lit {
                        queue = quote! { #s };
                    }
                }
                "max_attempts" => {
                    if let Lit::Int(i) = lit {
                        max_attempts = quote! { #i };
                    }
                }
                _ => {}
            }
        }
    }

    // Extract the perform method from the input implementation
    let mut perform_method = None;
    let mut backoff_method = None;

    for item in input.items.iter() {
        if let syn::ImplItem::Method(method) = item {
            match method.sig.ident.to_string().as_str() {
                "perform" => perform_method = Some(method.clone()),
                "backoff" => backoff_method = Some(method.clone()),
                _ => {}
            }
        }
    }

    // Generate the code
    let expanded = quote! {
        #[async_trait::async_trait]
        #[typetag::serde]
        impl Worker for #ty {
            fn queue(&self) -> &'static str {
                #queue
            }

            fn max_attempts(&self) -> i32 {
                #max_attempts
            }

            #backoff_method

            #perform_method
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn job(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);

    // Find and remove any existing derive attributes, collecting user derives
    let mut user_derives = Vec::new();
    let mut errors = Vec::new();

    input.attrs.retain(|attr| {
        if attr.path.is_ident("derive") {
            // Extract user-specified derives
            if let Ok(Meta::List(list)) = attr.parse_meta() {
                for nested in &list.nested {
                    if let NestedMeta::Meta(Meta::Path(path)) = nested {
                        if let Some(segment) = path.segments.first() {
                            let ident = segment.ident.to_string();
                            match ident.as_str() {
                                "Deserialize" | "Serialize" => {
                                    errors.push(syn::Error::new(
                                        segment.ident.span(),
                                        "The #[ishikari::job] macro automatically derives serde traits. Please remove the manual serde derives.",
                                    ));
                                }
                                _ => user_derives.push(ident),
                            }
                        }
                    }
                }
            }
            false // Remove the original derive attribute
        } else {
            true // Keep other attributes
        }
    });

    // If we found any errors, return them
    if !errors.is_empty() {
        let first_error = errors.remove(0);
        let error = errors.into_iter().fold(first_error, |mut acc, err| {
            acc.combine(err);
            acc
        });
        return error.to_compile_error().into();
    }

    let mut all_derives = vec!["Debug".to_string()];

    // Add user derives that aren't already included
    for derive in user_derives {
        if !all_derives.contains(&derive) {
            all_derives.push(derive);
        }
    }

    // Create a new derive attribute with all derives
    let derive_tokens = all_derives.iter().map(|derive| {
        let ident = Ident::new(derive, proc_macro2::Span::call_site());
        quote! { #ident }
    });

    let expanded = quote! {
        #[derive(#(#derive_tokens,)* ::serde::Serialize, ::serde::Deserialize)]
        #[serde(crate = "::ishikari::serde")]
        #input
    };

    expanded.into()
}
