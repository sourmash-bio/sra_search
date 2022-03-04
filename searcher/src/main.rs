use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::{Parser, Subcommand};
use log::{debug, error, info};
use rayon::prelude::*;
use sourmash::signature::{Signature, SigsTrait};
use sourmash::sketch::minhash::{max_hash_for_scaled, KmerMinHash};
use sourmash::sketch::nodegraph::Nodegraph;
use sourmash::sketch::Sketch;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Search {
        /// List of queries (one sig path per line in the file)
        #[clap(parse(from_os_str))]
        querylist: PathBuf,

        /// List of signatures to search
        #[clap(parse(from_os_str))]
        siglist: PathBuf,

        /// List of Nodegraphs matching the signature list
        #[clap(parse(from_os_str))]
        nglist: Option<PathBuf>,

        /// ksize
        #[clap(short, long, default_value = "31")]
        ksize: u8,

        /// threshold
        #[clap(short, long, default_value = "0.85")]
        threshold: f64,

        /// scaled
        #[clap(short, long, default_value = "1000")]
        scaled: usize,

        /// The path for output
        #[clap(parse(from_os_str), short, long)]
        output: Option<PathBuf>,
    },
    Prepare {
        /// List of signatures to search
        #[clap(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[clap(short, long, default_value = "31")]
        ksize: u8,

        /// scaled
        #[clap(short, long, default_value = "1000")]
        scaled: usize,

        /// The path for output
        #[clap(parse(from_os_str), short, long)]
        output_dir: PathBuf,
    },
}

fn search<P: AsRef<Path>>(
    querylist: P,
    siglist: P,
    nglist: Option<P>,
    threshold: f64,
    ksize: u8,
    scaled: usize,
    output: Option<P>,
) -> Result<(), anyhow::Error> {
    info!("Loading queries");

    let querylist_file = BufReader::new(File::open(querylist)?);

    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    let template = Sketch::MinHash(template_mh);

    let queries: Vec<(String, KmerMinHash, Nodegraph)> = querylist_file
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .filter_map(|query| {
            let query_sig = Signature::from_path(query).unwrap();

            let mut query = None;
            for sig in &query_sig {
                if let Some(Sketch::MinHash(mh)) = sig.select_sketch(&template) {
                    let mut ng = Nodegraph::with_tables(1024 * 1024, 1, ksize.into());
                    mh.iter_mins().for_each(|h| {
                        ng.count(*h);
                    });
                    query = Some((sig.name(), mh.clone(), ng));
                }
            }
            query
        })
        .collect();

    if queries.is_empty() {
        info!("No query signatures loaded, exiting.");
        return Ok(());
    }

    info!("Loaded {} query signatures", queries.len());

    info!("Loading siglist");
    let siglist_file = BufReader::new(File::open(siglist)?);
    let search_sigs: Vec<PathBuf> = siglist_file
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect();
    info!("Loaded {} sig paths in siglist", search_sigs.len());

    let cache_sigs = if let Some(nglist) = nglist {
        info!("Loading sig cache");
        let siglist_file = BufReader::new(File::open(nglist)?);
        let cache_sigs: Vec<Option<PathBuf>> = siglist_file
            .lines()
            .map(|line| {
                let mut path = PathBuf::new();
                path.push(line.unwrap());
                Some(path)
            })
            .collect();
        info!("Loaded {} cache sigs paths", cache_sigs.len());
        assert_eq!(cache_sigs.len(), search_sigs.len());
        cache_sigs
    } else {
        vec![None; search_sigs.len()]
    };

    let processed_sigs = AtomicUsize::new(0);

    let (send, recv) = std::sync::mpsc::sync_channel(rayon::current_num_threads());

    // Spawn a thread that is dedicated to printing to a buffered output
    let out: Box<dyn Write + Send> = match output {
        Some(path) => Box::new(BufWriter::new(File::create(path).unwrap())),
        None => Box::new(std::io::stdout()),
    };
    let thrd = std::thread::spawn(move || {
        let mut writer = BufWriter::new(out);
        writeln!(&mut writer, "query,Run,containment").unwrap();
        for (query, m, containment) in recv.into_iter() {
            writeln!(&mut writer, "'{}','{}',{}", query, m, containment).unwrap();
        }
    });

    let send = search_sigs
        .into_par_iter()
        .zip(cache_sigs.into_par_iter())
        .filter_map(|(filename, cache_filename)| {
            let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
            if i % 1000 == 0 {
                info!("Processed {} search sigs", i);
            }

            // load cache sig if available
            let cache_sig: Option<Nodegraph> = if let Some(filename) = cache_filename {
                Nodegraph::from_path(filename).ok()
            } else {
                None
            };

            let matches: Vec<_> = queries
                .iter()
                .filter_map(|(name, query, ng)| {
                    if let Some(cache_sig) = &cache_sig {
                        if ng.containment(cache_sig) >= threshold {
                            Some((name, query))
                        } else {
                            None
                        }
                    } else {
                        // No cache, we will have to load the MH from sig later
                        Some((name, query))
                    }
                })
                .collect();

            let mut search_mh = None;
            let search_sig = &Signature::from_path(&filename)
                .unwrap_or_else(|_| panic!("Error processing {:?}", filename))[0];
            if let Some(Sketch::MinHash(mh)) = search_sig.select_sketch(&template) {
                search_mh = Some(mh);
            }
            let search_mh = search_mh.unwrap();

            let match_fn = filename.clone().into_os_string().into_string().unwrap();
            let mut results = vec![];

            matches.into_iter().for_each(|(name, query)| {
                let containment =
                    query.count_common(search_mh, false).unwrap() as f64 / query.size() as f64;
                if containment > threshold {
                    results.push((name.clone(), match_fn.clone(), containment))
                }
            });

            if results.is_empty() {
                None
            } else {
                Some(results)
            }
        })
        .flatten()
        .try_for_each_with(send, |s, m| s.send(m));

    if let Err(e) = send {
        error!("Unable to send internal data: {:?}", e);
    }

    if let Err(e) = thrd.join() {
        error!("Unable to join internal thread: {:?}", e);
    }

    Ok(())
}

fn prepare<P: AsRef<Path>>(
    siglist: P,
    ksize: u8,
    scaled: usize,
    output_dir: P,
) -> Result<(), anyhow::Error> {
    info!("Loading queries");

    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    let template = Sketch::MinHash(template_mh);

    info!("Loading siglist");
    let siglist_file = BufReader::new(File::open(siglist)?);
    let search_sigs: Vec<PathBuf> = siglist_file
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect();
    info!("Loaded {} sig paths in siglist", search_sigs.len());

    let output_dir = output_dir.as_ref();
    if output_dir.exists() {
        debug!(
            "Path {} already exists; proceeding anyway",
            &output_dir.display()
        );
    } else {
        std::fs::create_dir_all(&output_dir)?;
    }
    let outpath = output_dir.to_path_buf();

    let processed_sigs = AtomicUsize::new(0);

    search_sigs.par_iter().try_for_each(|filename| {
        let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
        if i % 1000 == 0 {
            info!("Processed {} search sigs", i);
        }

        let mut search_mh = None;
        let search_sig = &Signature::from_path(&filename)
            .unwrap_or_else(|_| panic!("Error processing {:?}", filename))[0];
        if let Some(Sketch::MinHash(mh)) = search_sig.select_sketch(&template) {
            search_mh = Some(mh);
        }
        let search_mh = search_mh.unwrap();

        // TODO: tweak values
        let mut ng = Nodegraph::with_tables(1024 * 1024, 1, ksize.into());
        search_mh.iter_mins().for_each(|h| {
            ng.count(*h);
        });

        let mut outpath = outpath.clone();
        outpath.push(filename.file_name().unwrap());

        let mut outfile = BufWriter::new(File::create(outpath)?);
        let mut writer = niffler::get_writer(
            Box::new(&mut outfile),
            niffler::compression::Format::Gzip,
            niffler::compression::Level::One,
        )?;

        ng.save_to_writer(&mut writer)?;

        Ok(())
    })
}

fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    use Commands::*;

    let opts = Cli::parse();

    match opts.command {
        Search {
            querylist,
            nglist,
            siglist,
            threshold,
            ksize,
            scaled,
            output,
        } => search(querylist, siglist, nglist, threshold, ksize, scaled, output),
        Prepare {
            siglist,
            ksize,
            scaled,
            output_dir,
        } => prepare(siglist, ksize, scaled, output_dir),
    }
}
