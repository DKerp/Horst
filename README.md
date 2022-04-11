# Horst
A fast key-value store with ACID guarantees written in pure Rust. Heavily inspired by [Badger](https://github.com/dgraph-io/badger).

This is a __work in progress__. Use it at your own risk.

## Status

The database does currently not support durability. That means your data will be lost after a restart. You also have to manually delete the data from the directory. There is also no shapshot creation mechanism, which means outdated data is never deleted. Finally there is currently no way to delete a certain key from the database.

Everything else is working correctly and can in theory already be used.

## Implementation

The implementation closely follows the way [Badger](https://github.com/dgraph-io/badger) got implemented, as the [Dgraph](https://dgraph.io) team explained [in their blog post](https://dgraph.io/blog/post/badger-txn/). Given so Horst's transactions offer the some ACID guarantees as [Badger](https://github.com/dgraph-io/badger) does.

We maintain the list of pending commits s in sorted `Vec` containing `(commit_ts, finished)` elements. If a commit finishes successfully `finished` gets set to `true`, if it fails the entry gets removed. Once all transactions up to a certain `commit_ts` have been marked as finished, they all get removed from the `Vec` and the oracle's `read_ts` gets increased to that `commit_ts`.

The value log is implemented in such a way that each `VLog` file contains solely the transactions corresponding to a certain interval of commit timestamps which is disjunct with the intervals of all other `VLog` files. This ensures we can easily detect after the creation of a snapshot which `VLog` files can be deleted and which must be preserved.

The LSM tree uses a similiar system. Each `Slice` (or 'rune' as it is called on Wikipedia) contains solely the key-value pairs corresponding to a distinct interval of commit timestamps. The `Slice`s are in that sense sorted, with older `Slice`s from higher levels containing solely older values and newer `Slice`s containing solely newer values. This ensures that once we find the first key-value pair with a `commit_ts` lower then the transaction's `read_ts`, we can be sure the older `Slice`s will not contain a newer value and end the search early.

Contrary to [Badger](https://github.com/dgraph-io/badger) which uses byte slices of arbitrary length as keys, we use fixed sized keys of type `u128`. You can still use others keys, but you would have to convert them to `u128` before adding them to the database. You should also make sure there are no key collisions. We will likely add some interface and utility functionalites to assist with that.

### TODO

- Oracle
  - [x] Implement a seperate task which manages the oracle.
  - [x] Let the oracle keep track of commit timestamps for all keys.
    - We use a sorted `Vec` for quick search capabilites and to keep the memory usage low.
    - A `BTreeMap` gets used for quickly adding new key/commit_ts pairs.
    - If the `BTreeMap` gets too large, we merge it with the main `Vec` store.
    - If a certain commit updates too many keys, it gets directly merged into the main `Vec` store.
    - Upon conflict detection, both stores are checked for commit_ts higher then the transactions read_ts.
  - [x] Let the oracle keep track of pending transaction commits.
    - Successfully commited transactions get saved in a `Vec` with both the `commit_ts` and a `finished` flag defaulting to false.
    - If a transaction successfully completes a commit, it informs the oracle which will set the `finished` flag to `false`.
    - If a transaction fails, the oracle gets also informed and the corresponding entry gets deleted from the store.
    - After each successfull commit the oracle removes all entries starting from the beginning which consecutively(!) have the `finished` flag set to true. The oracle's `read_ts` gets updated to the highest `commit_ts` where there are no pending commits with a lower `commit_ts`.
  - [x] Provide the interface for letting other tasks and transactions communicate with the oracle.
    - [x] Allow asking for the latest `read_ts`.
    - [x] Allow asking for commiting a transaction.
      - [x] Send over all keys read or set by the transaction.
      - [x] Perform conflict detection by ensuring none of the keys read or set got modified.
- Value log
  - [x] Implement a seperate task which manages the value log.
  - [x] Catch incomming transactions and write them in batches for better write performance.
    - [x] Implement a buffer for incomming transactions.
    - [x] Empty the buffer at a configuable interval.
    - [ ] Empty the buffer if a configurable size gets exceeded.
    - [ ] Make sure the transactions are given to the correct `VLog`s (s.b.).
  - [x] Implement the management for opening mulitple `VLog` files.
  - [ ] Allow creating `VLog` files in multiple directories, e.g. on different harddrives.
  - [ ] Open multiple `VLog` files.
    - [ ] Implement a seperate taks which manages each Individual `VLog` file.
    - [ ] Allow for concurrent reads from different `VLog` files.
  - [ ] Make each `VLog` file contain the transactions corresponding to a certain interval of `commit_ts` which is disjunct with all other `VLog`s.
    - [ ] Let the header of each `VLog` file maintain the upper and lower bound (inclusive) of the commit timestamps contained therein.
    - [ ] The first `VLog`'s lower bound gets initialized to zero.
    - [ ] Upon writing transactions to a `VLog`, maintain an index of the highest `commit_ts` seen so far on this `VLog`.
    - [ ] Upon closure of the `VLog`, update the headers upper bound to the highest seen `commit_ts`. Do also save the upper bounds fixed flag (s.b.).
    - [ ] If a `VLog` exceeds a certain size limit, create a new `VLog`.
      - [ ] The last `VLog`'s upper bound gets marked as fixed.
      - [ ] The new `VLog`'s lower bound gets set as the last `VLog`'s upper bound +1.
    - [ ] Make sure that all new transactions get written to the correct `VLog`.
- Log-Structure-Merge (LSM) tree
  - [x] Implement a seperate task which manages the LSM tree.
  - [x] Implement a seperate task for each `Slice` and the level 0 storage.
  - [x] Implement level 0 (RAM storage).
    - [x] Store new key-value-pairs in a `BTreeMap`.
    - [x] Regularely merge the new keys into a sorted `Vec` for better memory usage.
    - [ ] Do also trigger a merge whenever a configurable size gets exceeded.
    - [x] Implement a configurable maximum total number of entries in level 0.
      - [x] All entries get merged into the sorted `Vec` before further processing.
      - [x] Level 0 asks the oracle for the latest `read_ts` and splits its currently saved entries into those with a `commit_ts` above this `read_ts`, and those whose `commit_ts` is below or equal to it.
      - [x] Those with a `commit_ts` above the `read_ts` stay inside level 0, the rest get added to level 1.
  - [x] Implement higher levels (Disk storage).
    - [x] Implement `Slice`s which are immutable.
    - [x] Ensure we can efficiently read `Slice`s from disk.
      - [x] Make use of mmap. ([memmap](https://github.com/danburkert/memmap-rs))
      - [ ] Implement a pure rust alternative.
        - [x] Implement a caching file reader.
        - [ ] Solve bugs at certain corner edge cases.
    - [x] Make sure we can efficiently search for the latest value (with respect to a certain `read_ts`) of a certain key.
      - [x] Make sure each `Slice` contains a distinct interval of commit timestamps.
      - [x] Implement a properly working binary search.
      - [x] Different `Slice`s can be searched concurrently.
- Durability
  - [ ] Add a proper clean shutdown mechanism.
    - [ ] Add a flag to all file headers indicating if the file got properly closed.
    - [ ] Add a header entry to `VLog` files taking note of the last known fully commited `commit_ts`.
  - [ ] Reopen all files on restart.
    - [x] Reopen and add all `VLog` files on restart.
    - [ ] Add some validation to the `VLog` files header parsing to detect possible contract violations or similiar inconsistancies.
    - [ ] Reopen and add all `Slice` files on restart.
    - [ ] Add some validation to the `Slice` files header parsing to detect possible contract violations or similiar inconsistancies.
    - [ ] Detect not fully written transactions and truncate the corresponding `VLog` files.
    - [ ] Detect not fully commited transactions and update the `Slice`s as needed.
- Database functions
  - [ ] Implement a snapshot creation function, so outdated data gets removed.
  - [ ] Implement a delete key function by adding a corresponding flag.
  - [ ] Implement an async key range iteration function. (Usefull for prefix scanning)
  - [ ] Implement key conversion functionalites.
    - [ ] Add an interface for using abritrary keys (probably trough a trait).
    - [ ] Implement a utility structure which detects and prevents key collisions.
