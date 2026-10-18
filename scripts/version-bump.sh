#!/usr/bin/env bash
set -euo pipefail

# Bump every crate in the workspace together (cargo-edit's set-version updates the crates'
# dependencies on each other too). The test crate isn't published and keeps its version.
# Usage: scripts/version-bump.sh [major|minor|patch]
BUMP_TYPE="${1:-patch}"

cargo set-version --workspace --exclude only-ishikari --bump "$BUMP_TYPE"

VERSION=$(cd ishikari && cargo pkgid | cut -d# -f2 | cut -d: -f2)
echo "Bumped to version $VERSION"
