# fddf
Fast data dupe finder

This is a small Rust command-line program to find duplicate files in a directory
recursively.  It uses a thread pool to calculate file hashes in parallel.

Duplicates are found by checking size, then (Blake2) hashes of parts of files of
same size, then a byte-for-byte comparison.

## Build/install

Directly from crates.io with `cargo install fddf`.

From checkout:
```
cargo build --release
cargo run --release
```

## Usage

```
fddf [-s] [-t] [-S] [-m SIZE] [-M SIZE] [-v] <rootdir>

-s: report dupe groups in a single line
-t: produce a grand total
-S: don't scan recursively for each directory given
-f: check for files with given pattern only
-F: check for files with given regular expression only
-m: minimum size (default 1 byte)
-M: maximum size (default unlimited)
-v: verbose operation
```

By default, zero-length files are ignored, since there is no meaningful data to
be duplicated.  Pass `-m 0` to include them.

PRs welcome!
