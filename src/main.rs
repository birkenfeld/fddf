#[macro_use]
extern crate clap;
extern crate walkdir;
extern crate scoped_pool;
extern crate num_cpus;
extern crate sha1;
extern crate fnv;

use std::fs::File;
use std::io::{Read, Write, stderr};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::collections::hash_map::Entry;

fn hash_file(verbose: bool, fsize: u64, path: PathBuf, tx: Sender<(u64, PathBuf, [u8; 20])>) {
    let mut buf = [0u8; 4096];
    let mut sha = sha1::Sha1::new();
    if verbose {
        let _ = writeln!(stderr(), "Hashing {}...", path.display());
    }
    match File::open(&path) {
        Ok(mut fp) => {
            while let Ok(n) = fp.read(&mut buf) {
                if n == 0 { break; }
                sha.update(&buf[..n]);
            }
            let hash = sha.digest().bytes();
            tx.send((fsize, path, hash)).unwrap();
        }
        Err(e) => {
            let _ = writeln!(stderr(), "Error opening file {}: {}", path.display(), e);
        }
    }
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

    let pool = scoped_pool::Pool::new(num_cpus::get() + 1);

    let mut sizes = fnv::FnvHashMap::default();
    let mut inodes = fnv::FnvHashSet::default();
    let mut hashes = fnv::FnvHashMap::default();

    pool.scoped(|scope| {
        let (tx, rx) = channel();
        let hashref = &mut hashes;

        scope.execute(move ||
            for (size, path, hash) in rx.iter() {
                hashref.entry(hash).or_insert_with(Vec::new).push((size, path));
            }
        );

        enum Found {
            One(PathBuf),
            Multiple
        }

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

        for dir_entry in walkdir::WalkDir::new(root).follow_links(false) {
            if let Ok(dir_entry) = dir_entry {
                if let Ok(meta) = dir_entry.metadata() {
                    let fsize = meta.len();
                    if meta.is_file() && (zerolen || fsize != 0) && inodes.insert(dir_entry.ino()) {
                        process(fsize, dir_entry);
                    }
                }
            }
        }
    });

    let singleline = args.is_present("singleline");
    for (_, entries) in hashes {
        if entries.len() > 1 {
            if singleline {
                let last = entries.len() - 1;
                for (i, (_, path)) in entries.into_iter().enumerate() {
                    print!("{}", path.display());
                    if i < last {
                        print!(" ");
                    }
                }
            } else {
                println!("Size {} bytes:", entries[0].0);
                for (_, path) in entries {
                    println!("    {}", path.display());
                }
            }
            println!();
        }
    }
}
