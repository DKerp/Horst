use horst::*;

use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{Ordering, AtomicU64};

use tokio::sync::Barrier;
use tokio::runtime::Builder;

use rand::prelude::*;



fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("Horst worker")
        .build()
        .unwrap();

    runtime.block_on(async {
        main_inner().await;
    });
}

async fn main_inner() {
    pretty_env_logger::init();

    let mut config = Config::default();
    config.vlog_folder = "/home/anonymous/horst2/vlog".into();
    config.lsm_folder = "/home/anonymous/horst2/lsm".into();
    config.oracle_store_max_size = 10_000_000;
    config.oracle_buffer_max_size = 5_000_000;
    config.lsm_slices_per_level = 5;
    config.lsm_level_zero_max_size = 1_000_000;

    let db = DB::open(&config).await.unwrap();

    let size = 128usize;

    // Create x random key-value-pairs which we will try to reoptain later on.
    let start = Instant::now();

    let amount = 100_000usize;
    let mut rng = rand::thread_rng();
    let mut pairs = Vec::with_capacity(amount);
    for _ in 0..amount {
        let key: u128 = rng.gen();
        let mut value = vec![0u8; size];
        rng.fill_bytes(&mut value);
        pairs.push((key, value));
    }

    log::warn!("Creating {} pairs took {:?}", amount, start.elapsed());

    let start = Instant::now();
    let mut txn = db.new_txn().await;
    for (key, value) in pairs.iter() {
        txn.set(*key, value.clone()).unwrap();
    }
    log::warn!("Adding {} pairs to the Txn took {:?}", amount, start.elapsed());
    let start = Instant::now();
    txn.commit().await.unwrap();
    log::warn!("Commiting {} pairs to the DB took {:?}", amount, start.elapsed());

    let start = Instant::now();
    pairs.sort();
    log::warn!("Sorting {} pairs took {:?}", amount, start.elapsed());


    // Check that all keys got correctly saved in the first placed.
    {
        let counter = AtomicU64::new(0u64);
        let counter = Arc::new(counter);

        let total: u32 = pairs.len().try_into().unwrap();

        let bench_store: Vec<Duration> = Vec::with_capacity(pairs.len());
        let bench_store = Arc::new(Mutex::new(bench_store));

        let barrier = Arc::new(Barrier::new(pairs.len()+1));

        let start = Instant::now();

        for (key, value) in pairs.iter() {
            let key = *key;
            let value = value.clone();

            let bench_store = Arc::clone(&bench_store);

            let mut txn = db.new_read_only_txn().await;

            let barrier = barrier.clone();

            let counter = Arc::clone(&counter);

            tokio::spawn(async move {
                let start = Instant::now();

                // let mut not_found = false;

                match txn.get(key).await {
                    Ok(Some(value_cmp)) => {
                        if value==value_cmp {
                            // log::warn!("Key {} successfully retrieved!", key);
                            counter.fetch_add(1, Ordering::SeqCst);
                        } else {
                            log::error!("Key {} retrieved, but the value is different! ", key);
                            log::error!("value: {:?}", value);
                            log::error!("value_cmp: {:?}", value_cmp);
                        }
                    }
                    Ok(None) => {
                        log::error!("Key {} could not be found!", key);
                        // not_found = true;
                    }
                    Err(err) => {
                        log::error!("Key {} could not be retrieved. err: {:?}", key, err);
                    }
                }

                let elapsed = start.elapsed();

                // log::warn!("Get took {:?}", elapsed);

                {
                    let mut bench_store = bench_store.lock().unwrap();

                    bench_store.push(elapsed);
                }

                // if not_found {
                //     if let Err(err) = txn.get_debug(key).await {
                //         log::error!("Key {} could not be retrieved. err: {:?}", key, err);
                //     }
                // }

                barrier.wait().await;
            });
        }

        barrier.wait().await;
        let elapsed = start.elapsed();

        let bench_store = bench_store.lock().unwrap();

        let loading_min = bench_store.iter().map(|d| *d).min().unwrap();
        let loading_avg = bench_store.iter().map(|d| *d).sum::<Duration>()/total;
        let loading_max = bench_store.iter().map(|d| *d).max().unwrap();

        log::warn!("Loaded {} keys. min: {:?}, avg: {:?}, max: {:?}, total: {:?}.", total, loading_min, loading_avg, loading_max, elapsed);

        log::warn!("Retrieving old keys succeeded in {}/{} cases.", counter.load(Ordering::SeqCst), total);
    }


    let iter_store = vec![1usize, 10, 100, 1000, 10_000, 100_000];
    let iter_amount_store = vec![1_000_000usize, 100_000, 10_000, 1000, 100, 10];

    for _ in 0..10 {
        for (&iter, &iter_amount) in iter_store.iter().zip(iter_amount_store.iter()) {

            let mut rng = rand::thread_rng();

            let barrier = Arc::new(Barrier::new(iter+1));

            // Create the pairs before so they do not count into the benchmark.
            let mut pairs_store = Vec::with_capacity(iter);
            for _ in 0..iter {
                let mut pairs = Vec::with_capacity(iter_amount);

                for _ in 0..iter_amount {
                    let key: u128 = rng.gen();
                    let mut value = vec![0u8; size];
                    rng.fill_bytes(&mut value);
                    pairs.push((key, value));
                }

                pairs_store.push(pairs);
            }

            let bench_store = Vec::<BenchCommit>::with_capacity(iter);
            let bench_store = Mutex::new(bench_store);
            let bench_store = Arc::new(bench_store);

            log::warn!("----------------------------------------------");
            log::warn!("BENCHMARK");
            log::warn!("This time: Writing {} transactions with {} key-value-pairs each.", iter, iter_amount);

            let start = Instant::now();

            for i in 0..iter {
                log::info!("----- TRANSACTION #{} -----", i);

                let barrier = barrier.clone();

                let mut pairs = pairs_store.pop().unwrap();

                let mut txn = db.new_txn().await;

                let bench_store = Arc::clone(&bench_store);

                tokio::spawn(async move {
                    log::info!("Starting to write them to the txn...");

                    let start = Instant::now();
                    for (key, value) in pairs.drain(..) {
                        txn.set(key, value).unwrap();
                    }
                    let elapsed = start.elapsed();

                    log::info!("... done. Took {:?}", elapsed);

                    let txn_write = elapsed;

                    log::info!("Startin to commit the txn...");

                    let start = Instant::now();
                    let mut bench = txn.commit().await.unwrap();
                    let elapsed = start.elapsed();

                    log::info!("... done. Took {:?}", elapsed);

                    bench.txn_write = txn_write;
                    bench_store.lock().unwrap().push(bench);

                    barrier.wait().await;
                });
            }

            barrier.wait().await;
            let elapsed = start.elapsed();

            log::warn!("BENCHMARK DONE. Took {:?}", elapsed);

            let bench_store = bench_store.lock().unwrap();
            let bench_avg = BenchCommitAverage::from_slice(&bench_store);

            log::warn!("----- DETAILS -----");
            log::warn!("{:#?}", bench_avg);
        }
    }


    // let pairs = Arc::new(pairs);


    let mut first = true;
    loop {
        let counter = AtomicU64::new(0u64);
        let counter = Arc::new(counter);

        let total: u32 = pairs.len().try_into().unwrap();

        let bench_store: Vec<Duration> = Vec::with_capacity(pairs.len());
        let bench_store = Arc::new(Mutex::new(bench_store));

        let barrier = Arc::new(Barrier::new(pairs.len()+1));

        let start = Instant::now();

        for (key, _value) in pairs.iter() {
            let key = *key;
            // let value = value.clone();

            let bench_store = Arc::clone(&bench_store);

            let mut txn = db.new_read_only_txn().await;

            let barrier = barrier.clone();

            let counter = Arc::clone(&counter);

            // let pairs = Arc::clone(&pairs);

            tokio::spawn(async move {
                let start = Instant::now();

                // let mut not_found = false;

                // match txn.get(key).await {
                //     Ok(Some(value_cmp)) => {
                //         if value==value_cmp {
                //             // log::warn!("Key {} successfully retrieved!", key);
                //             counter.fetch_add(1, Ordering::SeqCst);
                //         } else {
                //             log::error!("Key {} retrieved, but the value is different! ", key);
                //             log::error!("value: {:?}", value);
                //             log::error!("value_cmp: {:?}", value_cmp);
                //         }
                //     }
                //     Ok(None) => {
                //         log::error!("Key {} could not be found!", key);
                //         // not_found = true;
                //     }
                //     Err(err) => {
                //         log::error!("Key {} could not be retrieved. err: {:?}", key, err);
                //     }
                // }

                match txn.find(key).await {
                    Ok(true) => {
                        // if value==value_cmp {
                        //     // log::warn!("Key {} successfully retrieved!", key);
                            counter.fetch_add(1, Ordering::SeqCst);
                        // } else {
                        //     log::error!("Key {} retrieved, but the value is different! ", key);
                        //     log::error!("value: {:?}", value);
                        //     log::error!("value_cmp: {:?}", value_cmp);
                        // }
                    }
                    Ok(false) => {
                        log::error!("Key {} could not be found!", key);
                        // not_found = true;
                    }
                    Err(err) => {
                        log::error!("Key {} could not be retrieved. err: {:?}", key, err);
                    }
                }

                let elapsed = start.elapsed();

                // log::warn!("Get took {:?}", elapsed);

                {
                    let mut bench_store = bench_store.lock().unwrap();

                    bench_store.push(elapsed);
                }

                // if not_found {
                //     let pos = pairs.binary_search_by_key(&key, |&(key, _)| key).unwrap();
                //     let pos = pos.checked_sub(5).unwrap_or(0);
                //     for pos in pos..pos+11 {
                //         if let Some(&(key, _)) = pairs.get(pos) {
                //             log::warn!("Key neighbours - pos: {}, key: {}", pos, key);
                //         }
                //     }
                //
                //     match txn.get_debug(key).await {
                //         Ok(Some(value_cmp)) => {
                //             if value==value_cmp {
                //                 log::warn!("Key {} successfully retrieved! DATABASE CORRUPTION!!!!!!", key);
                //                 // counter.fetch_add(1, Ordering::SeqCst);
                //             } else {
                //                 log::error!("Key {} retrieved, but the value is different! ", key);
                //                 // log::error!("value: {:?}", value);
                //                 // log::error!("value_cmp: {:?}", value_cmp);
                //             }
                //         }
                //         Ok(None) => {
                //             log::error!("Key {} could not be found!", key);
                //             // not_found = true;
                //         }
                //         Err(err) => {
                //             log::error!("Key {} could not be retrieved. err: {:?}", key, err);
                //         }
                //     }
                // }

                barrier.wait().await;
            });
        }

        barrier.wait().await;
        let elapsed = start.elapsed();

        let bench_store = bench_store.lock().unwrap();

        let loading_min = bench_store.iter().map(|d| *d).min().unwrap();
        let loading_avg = bench_store.iter().map(|d| *d).sum::<Duration>()/total;
        let loading_max = bench_store.iter().map(|d| *d).max().unwrap();

        log::warn!("Loaded {} keys. min: {:?}, avg: {:?}, max: {:?}, total: {:?}.", total, loading_min, loading_avg, loading_max, elapsed);

        log::warn!("Retrieving old keys succeeded in {}/{} cases.", counter.load(Ordering::SeqCst), total);

        // break;
        if first {
            first = false;
            log::warn!("---------- The LSM slice parts should now be cached... try it again! --------------");
        } else {
            break;
        }
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
}
