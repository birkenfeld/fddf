#[macro_use]
extern crate clap;
extern crate walkdir;
extern crate scoped_pool;
extern crate num_cpus;
extern crate blake2;
extern crate fnv;
extern crate unbytify;

use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom, stderr};
use std::path::PathBuf;
use std::sync::mpsc::{Sender, channel};
use std::collections::hash_map::Entry;
use blake2::{Blake2b, Digest};

fn err(path: &PathBuf, err: io::Error) {
    let _ = writeln!(stderr(), "Error processing file {}: {}", path.display(), err);
}

type HashSender = Sender<(u64, PathBuf, Vec<u8>)>;
type DupeSender = Sender<(u64, Vec<PathBuf>)>;

const BLOCKSIZE: usize = 4096;
const GAPSIZE: i64 = 102400;

fn hash_file_inner(path: &PathBuf) -> io::Result<Vec<u8>> {
    let mut buf = [0u8; BLOCKSIZE];
    let mut fp = File::open(&path)?;
    let mut digest = Blake2b::default();
    // When we compare byte-by-byte, we don't need to hash the whole file.
    // Instead, hash a block of 4kB, skipping 100kB.
    loop {
        match fp.read(&mut buf)? {
            0 => break,
            n => digest.input(&buf[..n]),
        }
        fp.seek(SeekFrom::Current(GAPSIZE))?;
    }
    Ok(digest.result().to_vec())
}

fn hash_file(verbose: bool, fsize: u64, path: PathBuf, tx: HashSender) {
    if verbose {
        let _ = writeln!(stderr(), "Hashing {}...", path.display());
    }
    match hash_file_inner(&path) {
        Ok(hash) => tx.send((fsize, path, hash)).unwrap(),
        Err(e) => err(&path, e),
    }
}

trait Candidate {
    fn read_block(&mut self) -> Result<usize, ()>;
    fn buf_equal(&self, other: &Self) -> bool;
    fn into_path(self) -> PathBuf;
}

struct FastCandidate {
    path: PathBuf,
    file: File,
    buf: [u8; BLOCKSIZE],
    n: usize,
}

struct SlowCandidate {
    path: PathBuf,
    pos: usize,
    buf: [u8; BLOCKSIZE],
    n: usize,
}

impl Candidate for FastCandidate {
    fn read_block(&mut self) -> Result<usize, ()> {
        match self.file.read(&mut self.buf) {
            Ok(n) => { self.n = n; Ok(n) },
            Err(e) => { err(&self.path, e); Err(()) }
        }
    }

    fn buf_equal(&self, other: &Self) -> bool {
        self.buf[..self.n] == other.buf[..other.n]
    }

    fn into_path(self) -> PathBuf {
        self.path
    }
}

impl Candidate for SlowCandidate {
    fn read_block(&mut self) -> Result<usize, ()> {
        match File::open(&self.path).and_then(|mut f| {
            f.seek(SeekFrom::Start(self.pos as u64)).and_then(|_| {
                f.read(&mut self.buf)
            })
        }) {
            Ok(n) => { self.n = n; self.pos += n; Ok(n) },
            Err(e) => { err(&self.path, e); Err(()) }
        }
    }

    fn buf_equal(&self, other: &Self) -> bool {
        self.buf[..self.n] == other.buf[..other.n]
    }

    fn into_path(self) -> PathBuf {
        self.path
    }
}

fn compare_files_inner<C: Candidate>(fsize: u64, mut todo: Vec<C>, tx: &DupeSender) {
    'outer: loop {
        // Collect all candidates where buffer differs from the first.
        let mut todo_diff = Vec::new();
        for i in (1..todo.len()).rev() {
            if !todo[i].buf_equal(&todo[0]) {
                todo_diff.push(todo.swap_remove(i));
            }
        }
        // If there are enough of them, compare them among themselves.
        if todo_diff.len() >= 2 {
            // Note that they will compare their current buffer again as
            // the first step, which is exactly what we want.
            compare_files_inner(fsize, todo_diff, tx);
        }
        // If there are not enough candidates left, no dupes.
        if todo.len() < 2 {
            return;
        }
        // Read a new block of data from all files.
        for i in (0..todo.len()).rev() {
            match todo[i].read_block() {
                // If we're at EOF, all remaining are dupes.
                Ok(0) => break 'outer,
                // If an error occurs, do not process this file further.
                Err(_) => { todo.remove(i); }
                _ => ()
            }
        }
    }
    // We are finished and have more than one file in the candidate list.
    tx.send((fsize, todo.into_iter().map(Candidate::into_path).collect())).unwrap();
}

fn compare_files(verbose: bool, fsize: u64, paths: Vec<PathBuf>, tx: DupeSender) {
    if verbose {
        for path in &paths {
            let _ = writeln!(stderr(), "Comparing {}...", path.display());
        }
    }
    // If there are too many candidates, we cannot process them opening all
    // files at the same time.
    if paths.len() < 100 {
        let todo = paths.into_iter().filter_map(|p| {
            match File::open(&p) {
                Ok(f) => Some(FastCandidate { path: p, file: f, buf: [0u8; BLOCKSIZE], n: 0 }),
                Err(e) => { err(&p, e); None }
            }
        }).collect();
        compare_files_inner(fsize, todo, &tx);
    } else {
        let todo = paths.into_iter().map(|p| {
            SlowCandidate { path: p, pos: 0, buf: [0u8; BLOCKSIZE], n: 0 }
        }).collect();
        compare_files_inner(fsize, todo, &tx);
    }
}

fn validate_byte_size(s: String) -> Result<(), String> {
    unbytify::unbytify(&s).map(|_| ()).map_err(
        |_| format!("{:?} is not a byte size", s))
}

fn main() {
    let args = clap_app!(fddf =>
        (version: crate_version!())
        (author: "Georg Brandl, 2017")
        (about: "A parallel duplicate file finder.")
        (@arg minsize: -m [MINSIZE] default_value("1") validator(validate_byte_size)
         "Minimum file size to consider")
        (@arg maxsize: -M [MAXSIZE] validator(validate_byte_size)
         "Maximum file size to consider")
        (@arg total: -t "Report a grand total of duplicates?")
        (@arg singleline: -s "Report dupes on a single line?")
        (@arg verbose: -v "Verbose operation?")
        (@arg root: +required +multiple "Root directory or directories to search.")
    ).get_matches();

    let singleline = args.is_present("singleline");
    let grandtotal = args.is_present("total");
    let verbose = args.is_present("verbose");
    let minsize = unbytify::unbytify(args.value_of("minsize").unwrap()).unwrap();
    let maxsize = args.value_of("maxsize").map_or(u64::max_value(),
                                                  |v| unbytify::unbytify(v).unwrap());
    let roots = args.values_of("root").unwrap();

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
        // are uninteresting and ignored.
        for root in roots {
            for dir_entry in walkdir::WalkDir::new(root).follow_links(false) {
                match dir_entry {
                    Ok(dir_entry) => {
                        if dir_entry.file_type().is_file() {
                            match dir_entry.metadata() {
                                Ok(meta) => {
                                    let fsize = meta.len();
                                    if fsize >= minsize && fsize <= maxsize {
                                        // We take care to avoid visiting a single inode twice,
                                        // which takes care of (false positive) hardlinks.
                                        if inodes.insert(dir_entry.ino()) {
                                            process(fsize, dir_entry);
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = writeln!(stderr(), "{}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = writeln!(stderr(), "{}", e);
                    }
                }
            }
        }
    });

    let mut total_dupes = 0;
    let mut total_files = 0;
    let mut total_size = 0;

    {
        // Present results to the user.
        let mut print_dupe = |out: &mut io::StdoutLock, size, entries: Vec<PathBuf>| {
            total_dupes += 1;
            total_files += entries.len() - 1;
            total_size += size * (entries.len() - 1) as u64;
            if singleline {
                let last = entries.len() - 1;
                for (i, path) in entries.into_iter().enumerate() {
                    write!(out, "{}", path.display()).unwrap();
                    if i < last {
                        write!(out, " ").unwrap();
                    }
                }
            } else {
                writeln!(out, "Size {} bytes:", size).unwrap();
                for path in entries {
                    writeln!(out, "    {}", path.display()).unwrap();
                }
            }
            writeln!(out).unwrap();
        };

        // Compare files with matching hashes byte-by-byte, using the same thread
        // pool strategy as above.
        pool.scoped(|scope| {
            let (tx, rx) = channel();

            // Print dupes as they come in.
            scope.execute(move || {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                for (size, entries) in rx.iter() {
                    print_dupe(&mut stdout, size, entries);
                }
            });

            // Compare found files with same size and hash byte-by-byte.
            for ((fsize, _), entries) in hashes {
                if entries.len() > 1 {
                    let txc = tx.clone();
                    scope.execute(move || compare_files(verbose, fsize, entries, txc));
                }
            }
        });
    }

    if grandtotal {
        println!("Overall results:");
        println!("    {} groups of duplicate files", total_dupes);
        println!("    {} files are duplicates", total_files);
        let (val, suffix) = unbytify::bytify(total_size);
        println!("    {:.1} {} of space taken by dupliates", val, suffix);
    }
}
