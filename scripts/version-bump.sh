#!/usr/bin/env bash
set -euo pipefail

# Bump every crate in the workspace together (cargo-edit's set-version updates the crates'
# dependencies on each other too). The test crate isn't published and keeps its version.
# Usage: scripts/version-bump.sh [major|minor|patch]
BUMP_TYPE="${1:-patch}"

cargo set-version --workspace --exclude only-ishikari --bump "$BUMP_TYPE"

VERSION=$(cd ishikari && cargo pkgid | cut -d# -f2 | cut -d: -f2)

# ishikari-wasm is a workspace of its own: its version, and its ishikari's.
cargo set-version --manifest-path ishikari-wasm/Cargo.toml "$VERSION"
perl -pi -e 's/^ishikari = \{ version = "[^"]*"/ishikari = { version = "'"$VERSION"'"/' ishikari-wasm/Cargo.toml
echo "Bumped to version $VERSION"
