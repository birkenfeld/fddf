#[macro_use]
extern crate clap;
extern crate walkdir;
extern crate scoped_pool;
extern crate num_cpus;
extern crate sha1;
extern crate fnv;

use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom, stderr};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::collections::hash_map::Entry;

fn hash_file(verbose: bool, fsize: u64, path: PathBuf,
             tx: Sender<(u64, PathBuf, [u8; 20])>) {
    let mut buf = [0u8; 4096];
    let mut sha = sha1::Sha1::new();
    if verbose {
        let _ = writeln!(stderr(), "Hashing {}...", path.display());
    }
    match File::open(&path) {
        Ok(mut fp) => {
            // Since we compare byte-by-byte anyway, we don't need to hash the
            // whole file.  Instead, hash a block of 4096 bytes every MB.
            while let Ok(n) = fp.read(&mut buf) {
                if n == 0 { break; }
                sha.update(&buf[..n]);
                let _ = fp.seek(SeekFrom::Current(1024 * 1024));
            }
            let hash = sha.digest().bytes();
            tx.send((fsize, path, hash)).unwrap();
        }
        Err(e) => {
            let _ = writeln!(stderr(), "Error opening file {}: {}", path.display(), e);
        }
    }
}

struct Candidate {
    file: File,
    path: PathBuf,
    buf: [u8; 4096],
    n: usize,
}

fn compare_files_inner(fsize: u64, mut todo: Vec<Candidate>, tx: &Sender<(u64, Vec<PathBuf>)>) {
    'outer: loop {
        // Collect all candidates where buffer differs from the first.
        let mut todo_diff = Vec::new();
        for i in (1..todo.len()).rev() {
            if &todo[i].buf[..todo[i].n] != &todo[0].buf[..todo[0].n] {
                todo_diff.push(todo.swap_remove(i));
            }
        }
        // If there are enough of them, compare them among themselves.
        if todo_diff.len() >= 2 {
            // Note that they will compare their current buffer again as
            // the first step, which is exactly what we want.
            compare_files_inner(fsize, todo_diff, tx);
        }
        // If there is only the first left here, no dupes.
        if todo.len() < 2 {
            return;
        }
        // Read a new block of data from all files.
        for cand in &mut todo {
            match cand.file.read(&mut cand.buf) {
                Ok(n) if n > 0 => cand.n = n,
                _ => break 'outer,
            }
        }
    }
    // We are finished and have more than one file in the candidate list.
    tx.send((fsize, todo.into_iter().map(|item| item.path).collect())).unwrap();
}

fn compare_files(verbose: bool, fsize: u64, paths: Vec<PathBuf>,
                 tx: Sender<(u64, Vec<PathBuf>)>) {
    if verbose {
        for path in &paths {
            let _ = writeln!(stderr(), "Comparing {}...", path.display());
        }
    }
    // Note: since all files were previously hashed, unwrap()ping the open here.
    let todo = paths.into_iter().map(|p| {
        Candidate { file: File::open(&p).unwrap(), path: p, buf: [0u8; 4096], n: 0 }
    }).collect::<Vec<_>>();
    compare_files_inner(fsize, todo, &tx);
}

fn main() {
    let args = clap_app!(fddf =>
        (version: crate_version!())
        (author: "Georg Brandl, 2017")
        (about: "A parallel duplicate file finder.")
        (@arg zerolen: -z "Report zero-length files?")
        (@arg singleline: -s "Report dupes on a single line?")
        (@arg verbose: -v "Verbose operation?")
        (@arg root: +required "Root directory to search.")
    ).get_matches();

    let zerolen = args.is_present("zerolen");
    let verbose = args.is_present("verbose");
    let root = args.value_of("root").unwrap();

    // See below for these maps' purpose.
    let mut sizes = fnv::FnvHashMap::default();
    let mut hashes = fnv::FnvHashMap::default();
    let mut inodes = fnv::FnvHashSet::default();

    // Set up thread pool for our various tasks.  Number of CPUs + 1 has been
    // found to be a good pool size, likely since the walker thread should be
    // doing mostly IO.
    let pool = scoped_pool::Pool::new(num_cpus::get() + 1);
    pool.scoped(|scope| {
        let (tx, rx) = channel();

        // One long-living job to collect hashes and populate the "hashes"
        // hashmap, received from the hashing jobs.  Only hashmap entries
        // with more than one vector element are duplicates in the end.
        let hashref = &mut hashes;
        scope.execute(move || {
            for (size, path, hash) in rx.iter() {
                hashref.entry((size, hash)).or_insert_with(Vec::new).push(path);
            }
        });

        enum Found {
            One(PathBuf),
            Multiple
        }

        // Processing a single file entry, with the "sizes" hashmap collecting
        // same-size files.  Entries are either Found::One or Found::Multiple,
        // so that we can submit the first file's path as a hashing job when the
        // first duplicate is found.  Hashing each file is submitted as a job to
        // the pool.
        let mut process = |fsize, dir_entry: walkdir::DirEntry| {
            let path = dir_entry.path().to_path_buf();
            match sizes.entry(fsize) {
                Entry::Vacant(v) => {
                    v.insert(Found::One(path));
                }
                Entry::Occupied(mut v) => {
                    let first = std::mem::replace(v.get_mut(), Found::Multiple);
                    if let Found::One(first_path) = first {
                        let txc = tx.clone();
                        scope.execute(move || hash_file(verbose, fsize, first_path, txc));
                    }
                    let txc = tx.clone();
                    scope.execute(move || hash_file(verbose, fsize, path, txc));
                }
            }
        };

        // The main thread just walks and filters the directory tree.  Symlinks
        // are uninteresting and ignored, as are any errors retrieving metadata.
        for dir_entry in walkdir::WalkDir::new(root).follow_links(false) {
            if let Ok(dir_entry) = dir_entry {
                if let Ok(meta) = dir_entry.metadata() {
                    let fsize = meta.len();
                    // We take care to avoid visiting a single inode twice,
                    // which takes care of (false positive) hardlinks.
                    if meta.is_file() && (zerolen || fsize != 0) && inodes.insert(dir_entry.ino()) {
                        process(fsize, dir_entry);
                    }
                }
            }
        }
    });

    // Compare files with matching hashes byte-by-byte, using the same thread
    // pool strategy as above.
    let mut dupes = Vec::new();
    pool.scoped(|scope| {
        let (tx, rx) = channel();

        let duperef = &mut dupes;
        scope.execute(move || duperef.extend(rx.iter()));

        // Compare found files with same size and hash byte-by-byte.
        for ((fsize, _), entries) in hashes {
            if entries.len() > 1 {
                let txc = tx.clone();
                scope.execute(move || compare_files(verbose, fsize, entries, txc));
            }
        }
    });

    // Present results to the user.
    let singleline = args.is_present("singleline");
    for (size, entries) in dupes {
        if singleline {
            let last = entries.len() - 1;
            for (i, path) in entries.into_iter().enumerate() {
                print!("{}", path.display());
                if i < last {
                    print!(" ");
                }
            }
        } else {
            println!("Size {} bytes:", size);
            for path in entries {
                println!("    {}", path.display());
            }
        }
        println!();
    }
}
