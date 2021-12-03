use std::fs::{File, Metadata};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::{Sender, channel};
use std::collections::hash_map::Entry;
use structopt::StructOpt;
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use blake3::Hasher;
use walkdir::{DirEntry, WalkDir};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use regex::Regex;
use glob::Pattern;

fn err(path: &PathBuf, err: io::Error) {
    eprintln!("Error processing file {}: {}", path.display(), err);
}

type HashSender = Sender<(u64, PathBuf, Vec<u8>)>;
type DupeSender = Sender<(u64, Vec<PathBuf>)>;

const BLOCKSIZE: usize = 4096;
const GAPSIZE: i64 = 102_400;

fn hash_file_inner(path: &PathBuf) -> io::Result<Vec<u8>> {
    let mut buf = [0u8; BLOCKSIZE];
    let mut fp = File::open(&path)?;
    let mut digest = Hasher::new();
    // When we compare byte-by-byte, we don't need to hash the whole file.
    // Instead, hash a block of 4kB, skipping 100kB.
    loop {
        match fp.read(&mut buf)? {
            0 => break,
            n => digest.update(&buf[..n]),
        };
        fp.seek(SeekFrom::Current(GAPSIZE))?;
    }
    Ok(digest.finalize().as_bytes().to_vec())
}

fn hash_file(verbose: bool, fsize: u64, path: PathBuf, tx: HashSender) {
    if verbose {
        eprintln!("Hashing {}...", path.display());
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
            eprintln!("Comparing {}...", path.display());
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

#[derive(StructOpt)]
#[structopt(about="A parallel duplicate file finder.")]
struct Args {
    #[structopt(short="m", default_value="1", parse(try_from_str=unbytify::unbytify),
                help="Minimum file size to consider")]
    minsize: u64,
    #[structopt(short="M", parse(try_from_str=unbytify::unbytify),
                help="Maximum file size to consider")]
    maxsize: Option<u64>,
    #[structopt(short="H", help="Exclude Unix hidden files (names starting with dot)")]
    nohidden: bool,
    #[structopt(short="S", help="Don't scan recursively in directories?")]
    nonrecursive: bool,
    #[structopt(short="t", help="Report a grand total of duplicates?")]
    grandtotal: bool,
    #[structopt(short="s", help="Report dupes on a single line?")]
    singleline: bool,
    #[structopt(short="v", help="Verbose operation?")]
    verbose: bool,
    #[structopt(short="0", help="With -s, separate dupes with NUL, replace newline with two NULs")]
    nul: bool,
    #[structopt(short="f", help="Check only filenames matching this pattern", group="patterns")]
    pattern: Option<Pattern>,
    #[structopt(short="F", help="Check only filenames matching this regexp", group="patterns")]
    regexp: Option<Regex>,
    #[structopt(help="Root directory or directories to search")]
    roots: Vec<PathBuf>,
}

fn is_hidden_file(entry: &DirEntry) -> bool {
    entry.file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn main() {
    let Args { minsize, maxsize, verbose, singleline, grandtotal, nohidden,
               nonrecursive, nul, pattern, regexp, roots } = Args::from_args();
    let maxsize = maxsize.unwrap_or(u64::max_value());

    enum Select {
        Pattern(Pattern),
        Regex(Regex),
        Any,
    }

    let select = if let Some(pat) = pattern {
        Select::Pattern(pat)
    } else if let Some(regex) = regexp {
        Select::Regex(regex)
    } else {
        Select::Any
    };

    let hidden_excluded = |entry: &DirEntry| nohidden && is_hidden_file(entry);

    let matches_pattern = |entry: &DirEntry| match select {
        Select::Any => true,
        Select::Pattern(ref p) => entry.file_name().to_str().map_or(false, |f| p.matches(f)),
        Select::Regex(ref r) => entry.file_name().to_str().map_or(false, |f| r.is_match(f)),
    };

    // See below for these maps' purpose.
    let mut sizes = HashMap::default();
    let mut hashes = HashMap::default();
    let mut inodes = HashSet::default();

    // We take care to avoid visiting a single inode twice,
    // which takes care of (false positive) hardlinks.
    #[cfg(unix)]
    fn check_inode(set: &mut HashSet<(u64, u64)>, entry: &Metadata) -> bool {
        set.insert((entry.dev(), entry.ino()))
    }
    #[cfg(not(unix))]
    fn check_inode(_: &mut HashSet<(u64, u64)>, _: &Metadata) -> bool {
        true
    }

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
        let mut process = |fsize, dir_entry: DirEntry| {
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
        let roots = if roots.is_empty() { vec![".".into()] } else { roots };
        for root in roots {
            let walkdir = if nonrecursive {
                WalkDir::new(root).max_depth(1).follow_links(false)
            } else {
                WalkDir::new(root).follow_links(false)
            };
            for dir_entry in walkdir {
                match dir_entry {
                    Ok(dir_entry) => {
                        if dir_entry.file_type().is_file() {
                            match dir_entry.metadata() {
                                Ok(meta) => {
                                    let fsize = meta.len();
                                    if fsize >= minsize && fsize <= maxsize {
                                        if check_inode(&mut inodes, &meta) {
                                            if !hidden_excluded(&dir_entry) && matches_pattern(&dir_entry) {
                                                process(fsize, dir_entry);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("{}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", e);
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
                        if nul {
                            write!(out, "\0").unwrap();
                        } else {
                            write!(out, " ").unwrap();
                        }
                    }
                }
            } else {
                writeln!(out, "Size {} bytes:", size).unwrap();
                for path in entries {
                    writeln!(out, "    {}", path.display()).unwrap();
                }
            }

            if nul {
                write!(out, "\0\0").unwrap();
            } else {
                writeln!(out).unwrap();
            }
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
        println!("    {:.1} {} of space taken by duplicates", val, suffix);
    }
}
