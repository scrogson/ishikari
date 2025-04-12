use proc_macro::TokenStream;
use quote::*;
use syn::{
    parse_macro_input, AttributeArgs, DeriveInput, ItemImpl, Lit, Meta, MetaNameValue, NestedMeta,
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
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, lit, .. })) => {
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
            _ => {}
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

    proc_macro2::TokenStream::from(expanded).into()
}

#[proc_macro_attribute]
pub fn job(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    // Generate the code
    let expanded = quote! {
        #[serde(crate = "ishikari::serde")]
        #input
    };

    proc_macro2::TokenStream::from(expanded).into()
}
