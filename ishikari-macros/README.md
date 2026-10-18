# Ishikari Macros

The `#[job]` and `#[worker]` attribute macros for [Ishikari](https://crates.io/crates/ishikari).
Use them through `ishikari` (`#[ishikari::job]`, `#[ishikari::worker]`); don't depend on this
crate directly. Their expansion names only `ishikari`, so your crate needs no `serde`,
`typetag` or `async-trait` of its own.

## `#[job]`

Derives what a job type needs: `Debug`, `Serialize` and `Deserialize`, alongside any other
derives on the type. Don't derive `Serialize` or `Deserialize` yourself. It takes no
arguments.

```rust
#[ishikari::job]
#[derive(Clone)]
struct SendWelcome {
    user_id: i64,
}
```

## `#[worker]`

Implements `Worker` for a job type, keeping every method written in the impl. Its options set
the common methods without writing them out:

- `queue = "name"`: the queue jobs of this type go to.
- `max_attempts = N`: how many times a job is tried before it is discarded.
- `timeout = "30s"`: how long one attempt may run, in `ms`, `s`, `m` or `h`.

Setting an option both here and as a method is an error.

```rust
use ishikari::prelude::*;

#[ishikari::worker(queue = "email", max_attempts = 5, timeout = "30s")]
impl Worker for SendWelcome {
    async fn perform(&self, ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}
```

## License

MIT
