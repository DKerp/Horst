use horst2::*;

use std::time::Instant;
use std::convert::TryInto;
use std::sync::Arc;

use tokio::sync::Barrier;

use rand::prelude::*;


#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let db = DB::open("/home/anonymous/horst2").await.unwrap();

    // let mut txn = db.new_txn().await;
    //
    // match txn.get(0).await.unwrap() {
    //     Some(value) => log::info!("key '0' found: {}", String::from_utf8_lossy(&value)),
    //     None => log::info!("key '0' not found."),
    // }
    //
    // txn.set(0, "world".as_bytes().to_vec()).unwrap();
    //
    // txn.commit().await.unwrap();
    //
    // let mut txn = db.new_read_only_txn().await;
    //
    // match txn.get(0).await.unwrap() {
    //     Some(value) => log::info!("key '0' found: {}", String::from_utf8_lossy(&value)),
    //     None => log::info!("key '0' not found."),
    // }


    let amount = 1_000_000u128;
    let size = 128usize;

    // for _ in 0..2usize {
    //     log::info!("BENCHMARK");
    //     log::info!("Creating {} key/value pairs...", amount);
    //
    //     let mut rng = rand::thread_rng();
    //
    //     let mut pairs = Vec::with_capacity(amount.try_into().unwrap());
    //     for _ in 0..amount {
    //         // let i: u64 = rng.gen();
    //         // let key = i.to_be_bytes().to_vec();
    //         let key: u128 = rng.gen();
    //         // let temp: [u8; 64] = rng.gen();
    //         let value = vec![3u8; size];
    //         pairs.push((key, value));
    //     }
    //
    //     log::info!("... done.");
    //
    //     let mut txn = db.new_txn().await;
    //
    //     log::info!("Starting to write them to the txn...");
    //
    //     let start = Instant::now();
    //     for (key, value) in pairs.drain(..) {
    //         txn.set(key, value).unwrap();
    //     }
    //     let elapsed = start.elapsed();
    //
    //     log::info!("... done. Took {:?}", elapsed);
    //
    //     log::info!("Startin to commit the txn...");
    //
    //     let start = Instant::now();
    //     txn.commit().await.unwrap();
    //     let elapsed = start.elapsed();
    //
    //     log::info!("... done. Took {:?}", elapsed);
    // }

    /* Try writing the values in many transactions */

    // let mut counter = 1u128;

    for &(iter, iter_amount) in [(10u32, 100_000u32), (100, 10_000), (1000, 1000), (100, 10_000), (1000, 1000), (100, 10_000), (10, 100_000)].iter() {
    // for &(iter, iter_amount) in [(10u32, 100_000u32), (10u32, 100_000u32)].iter() {
    // for &(iter, iter_amount) in [(100u32, 10_000u32), (100u32, 10_000u32)].iter() {
    // for &(iter, iter_amount) in [(1000u32, 1_000u32), (1000u32, 1_000u32)].iter() {
    // for &(iter, iter_amount) in [(10_000u32, 100u32), (10_000u32, 100u32)].iter() {

        log::warn!("BENCHMARK");
        log::warn!("This time: Writing {} transactions with {} key-value-pairs each.", iter, iter_amount);

        let mut rng = rand::thread_rng();

        let barrier = Arc::new(Barrier::new(iter as usize+1));

        let start = Instant::now();

        for i in 0..iter {
            log::info!("----- TRANSACTION #{} -----", i);

            let barrier = barrier.clone();

            let mut pairs = Vec::with_capacity(amount.try_into().unwrap());

            for _ in 0..iter_amount {
                let key: u128 = rng.gen();
                let value = vec![3u8; size];
                pairs.push((key, value));
            }

            let mut txn = db.new_txn().await;

            // counter = counter+1;
            // let commit_ts = counter;

            tokio::spawn(async move {
                log::info!("Starting to write them to the txn...");

                let start = Instant::now();
                for (key, value) in pairs.drain(..) {
                    txn.set(key, value).unwrap();
                }
                let elapsed = start.elapsed();

                log::info!("... done. Took {:?}", elapsed);

                log::info!("Startin to commit the txn...");

                let start = Instant::now();
                txn.commit().await.unwrap();
                let elapsed = start.elapsed();

                log::info!("... done. Took {:?}", elapsed);

                barrier.wait().await;
            });
        }

        barrier.wait().await;
        let elapsed = start.elapsed();
        tokio::time::sleep(std::time::Duration::new(5, 0)).await;
        log::warn!("BENCHMARK DONE. Took {:?}", elapsed);
    }

    loop {
        tokio::time::sleep(std::time::Duration::new(10, 0)).await;
    }
}
