#!/usr/bin/env bash
set -euo pipefail

# Bump every crate in the workspace together (cargo-edit's set-version updates the crates'
# dependencies on each other too). The test crate isn't published and keeps its version.
# Usage: scripts/version-bump.sh [major|minor|patch]
BUMP_TYPE="${1:-patch}"

cargo set-version --workspace --exclude only-ishikari --bump "$BUMP_TYPE"

VERSION=$(cd ishikari && cargo pkgid | cut -d# -f2 | cut -d: -f2)

# ishikari-wasm is a workspace of its own: its ishikari and ishikari-admin first (set-version
# can't resolve it while they ask for the old version), then its own version.
perl -pi -e 's/^(ishikari(?:-admin)?) = \{ version = "[^"]*"/$1 = { version = "'"$VERSION"'"/' ishikari-wasm/Cargo.toml
cargo set-version --manifest-path ishikari-wasm/Cargo.toml "$VERSION"
echo "Bumped to version $VERSION"
