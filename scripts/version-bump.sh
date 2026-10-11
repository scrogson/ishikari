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

# Bump ishikari, the CLI and the admin to match, and their dependencies on ishikari
for crate in ishikari ishikari-cli ishikari-admin; do
  (cd "$crate" && cargo set-version "$MACROS_VERSION")
done
sed -i '' "s/^ishikari = { version = \".*\", path = \"..\/ishikari\" }/ishikari = { version = \"$MACROS_VERSION\", path = \"..\/ishikari\" }/" ishikari-cli/Cargo.toml ishikari-admin/Cargo.toml

echo "Bumped to version $MACROS_VERSION"
