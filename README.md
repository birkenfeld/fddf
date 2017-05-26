# fddf
Fast data dupe finder

This is a small Rust command-line program to find duplicate files in a directory recursively.
It uses a thread pool to calculate file hashes in parallel.

Duplicates are found by checking size, then SHA-1.  No byte-for-byte comparison is made.

## Build and use

```
cargo build --release
cargo run --release
```

Not yet submitted to crates.io.
