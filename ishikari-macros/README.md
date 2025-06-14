# Ishikari Macros

This crate provides procedural macros for the Ishikari job processing framework.

## Macros

### `#[job]`

The `#[job]` attribute macro is used to derive job-related functionality for a struct. It automatically:

- Implements necessary traits for job serialization/deserialization
- Adds job-specific functionality

```rust
use ishikari::prelude::*;

#[derive(Debug)]
#[ishikari::job]
struct MyJob {
    // Your job data here
}
```

### `#[worker]`

The `#[worker]` attribute macro is used to implement the `Worker` trait for a type. It provides:

- Default implementations for queue configuration
- Retry and backoff strategy configuration
- Integration with the job processing system

```rust
use ishikari::prelude::*;

#[ishikari::worker(queue = "high_priority", max_attempts = 3)]
impl Worker for MyJob {
    async fn perform(&self, ctx: Context) -> PerformResult {
        // Your job processing logic here
        Complete::default().into()
    }
}
```

## Usage

This crate is automatically included as a dependency when you add `ishikari` to your project. You don't need to add it separately.

## License

MIT License
