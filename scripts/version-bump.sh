#!/usr/bin/env bash
set -euo pipefail

BUMP_TYPE="${1:-patch}"

# Bump macros version
cd ishikari-macros
cargo set-version --bump "$BUMP_TYPE"
MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
cd ..

# Update ishikari's dependency on macros
sed -i '' "s/ishikari-macros = { version = \".*\", path = \".*\"/ishikari-macros = { version = \"$MACROS_VERSION\", path = \"..\/ishikari-macros\"/" ishikari/Cargo.toml

# Bump ishikari version to match
cd ishikari
cargo set-version "$MACROS_VERSION"
cd ..

echo "Bumped to version $MACROS_VERSION"
