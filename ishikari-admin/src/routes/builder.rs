//! The workflow builder runs in the definition editor (`assets/builder.js`), so its old paths,
//! `/builder` and anything under it, go there.

use axum::{extract::State, response::Redirect};

use crate::AppState;

/// Redirect to the editor for a new definition.
pub async fn redirect(State(state): State<AppState>) -> Redirect {
    Redirect::to(&format!("{}/definitions/new", state.base_path))
}
