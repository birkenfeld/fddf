# fddf
Fast data dupe finder

This is a small Rust command-line program to find duplicate files in a directory recursively.
It uses a thread pool to calculate file hashes in parallel.

Duplicates are found by checking size, then SHA-1.  No byte-for-byte comparison is made.

## Build/install

Directly from crates.io with `cargo install fddf`.

From checkout:
```
cargo build --release
cargo run --release
```

## Usage

```
fddf [-s] [-z] <rootdir>

-s: report dupe groups in a single line
-z: report zero-length files a group of dupes
```

PRs welcome!
