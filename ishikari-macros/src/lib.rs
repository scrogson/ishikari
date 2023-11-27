use proc_macro::TokenStream;
use quote::*;
use syn::{parse_macro_input, DeriveInput, ItemImpl};

#[proc_macro_attribute]
pub fn worker(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    // Generate the code
    let expanded = quote! {
        #[async_trait::async_trait]
        #[typetag::serde]
        #input
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
