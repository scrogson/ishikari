# Default recipe to run when just is called without arguments
default:
    @just --list

# Set variables
set dotenv-load

# Version management
version-patch:
    #!/usr/bin/env bash
    # Bump macros version
    cd ishikari-macros
    cargo set-version --bump patch
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    # Update ishikari's dependency on macros
    sed -i '' "s/ishikari-macros = { version = \".*\", path = \".*\"/ishikari-macros = { version = \"$MACROS_VERSION\", path = \"..\/ishikari-macros\"/" ishikari/Cargo.toml
    # Bump ishikari version to match
    cd ishikari
    cargo set-version $MACROS_VERSION
    cd ..

version-minor:
    #!/usr/bin/env bash
    # Bump macros version
    cd ishikari-macros
    cargo set-version --bump minor
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    # Update ishikari's dependency on macros
    sed -i '' "s/ishikari-macros = { version = \".*\", path = \".*\"/ishikari-macros = { version = \"$MACROS_VERSION\", path = \"..\/ishikari-macros\"/" ishikari/Cargo.toml
    # Bump ishikari version to match
    cd ishikari
    cargo set-version $MACROS_VERSION
    cd ..

version-major:
    #!/usr/bin/env bash
    # Bump macros version
    cd ishikari-macros
    cargo set-version --bump major
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    # Update ishikari's dependency on macros
    sed -i '' "s/ishikari-macros = { version = \".*\", path = \".*\"/ishikari-macros = { version = \"$MACROS_VERSION\", path = \"..\/ishikari-macros\"/" ishikari/Cargo.toml
    # Bump ishikari version to match
    cd ishikari
    cargo set-version $MACROS_VERSION
    cd ..

# Build commands
build:
    cargo build

build-release:
    cargo build --release

# Run commands
admin:
    cargo run -p ishikari-admin

# Test commands
test:
    cargo test

test-watch:
    cargo watch -x test

# Linting and formatting
check:
    cargo check

clippy:
    cargo clippy -- -D warnings

fmt:
    cargo fmt

fmt-check:
    cargo fmt -- --check

# Publishing commands
publish-dry-run:
    #!/usr/bin/env bash
    echo "Dry running publish for ishikari-macros..."
    cd ishikari-macros
    cargo publish --dry-run --allow-dirty
    cd ..
    echo "Dry running publish for ishikari..."
    cd ishikari
    cargo publish --dry-run --allow-dirty
    cd ..

publish:
    #!/usr/bin/env bash
    echo "Publishing ishikari-macros..."
    cd ishikari-macros
    cargo publish
    cd ..
    echo "Publishing ishikari..."
    cd ishikari
    cargo publish
    cd ..

# Release process commands
release-bump-patch:
    #!/usr/bin/env bash
    echo "1. Running checks..."
    just fmt clippy test || exit 1
    
    echo "2. Bumping patch version..."
    just version-patch || exit 1
    
    echo "3. Verifying version updates..."
    echo "Checking ishikari-macros version..."
    cd ishikari-macros
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    
    echo "Checking ishikari version and dependency..."
    cd ishikari
    ISHIKARI_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    if [ "$MACROS_VERSION" != "$ISHIKARI_VERSION" ]; then
        echo "Error: Version mismatch between ishikari-macros ($MACROS_VERSION) and ishikari ($ISHIKARI_VERSION)"
        exit 1
    fi
    cd ..
    
    echo "4. Running dry-run publish..."
    just publish-dry-run || exit 1
    
    echo "5. If everything looks good, run: just publish"

release-bump-minor:
    #!/usr/bin/env bash
    echo "1. Running checks..."
    just fmt clippy test || exit 1
    
    echo "2. Bumping minor version..."
    just version-minor || exit 1
    
    echo "3. Verifying version updates..."
    echo "Checking ishikari-macros version..."
    cd ishikari-macros
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    
    echo "Checking ishikari version and dependency..."
    cd ishikari
    ISHIKARI_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    if [ "$MACROS_VERSION" != "$ISHIKARI_VERSION" ]; then
        echo "Error: Version mismatch between ishikari-macros ($MACROS_VERSION) and ishikari ($ISHIKARI_VERSION)"
        exit 1
    fi
    cd ..
    
    echo "4. Running dry-run publish..."
    just publish-dry-run || exit 1
    
    echo "5. If everything looks good, run: just publish"

release-bump-major:
    #!/usr/bin/env bash
    echo "1. Running checks..."
    just fmt clippy test || exit 1
    
    echo "2. Bumping major version..."
    just version-major || exit 1
    
    echo "3. Verifying version updates..."
    echo "Checking ishikari-macros version..."
    cd ishikari-macros
    MACROS_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    cd ..
    
    echo "Checking ishikari version and dependency..."
    cd ishikari
    ISHIKARI_VERSION=$(cargo pkgid | cut -d# -f2 | cut -d: -f2)
    if [ "$MACROS_VERSION" != "$ISHIKARI_VERSION" ]; then
        echo "Error: Version mismatch between ishikari-macros ($MACROS_VERSION) and ishikari ($ISHIKARI_VERSION)"
        exit 1
    fi
    cd ..
    
    echo "4. Running dry-run publish..."
    just publish-dry-run || exit 1
    
    echo "5. If everything looks good, run: just publish"

# Clean up
clean:
    cargo clean

# Development workflow
dev: fmt clippy test

# Release workflow
release: fmt clippy test build-release publish-dry-run

# Help
help:
    @echo "Available commands:"
    @just --list 