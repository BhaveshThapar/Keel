//! One set of assertions, run against every [`Store`].
//!
//! `MemStore` is what the simulator runs on and `LsmStore` is what a node runs
//! on, which means the two are never exercised by the same run — so a
//! difference between them shows up as a bug in whichever one the simulator is
//! not using, discovered on a real cluster. The same argument `keel-log` makes
//! about its filesystem seam and `keel-net` about its transport.
//!
//! Feature-gated, so a consumer that supplies its own store can be held to
//! exactly these assertions.

use bytes::Bytes;

use crate::store::{Batch, Space, Store};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

// The suite is library code, so it is held to the workspace's ban on `unwrap`
// and `expect` like everything else that ships. These do the same job and say
// which operation failed.

fn commit<S: Store>(store: &mut S, index: u64, batch: Batch) {
    if let Err(e) = store.commit(index, batch) {
        panic!("commit at index {index} failed: {e}");
    }
}

fn get<S: Store>(store: &S, space: Space, key: &[u8]) -> Option<Bytes> {
    match store.get(space, key) {
        Ok(value) => value,
        Err(e) => panic!("get of {key:?} failed: {e}"),
    }
}

fn scan<S: Store>(
    store: &S,
    space: Space,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    limit: usize,
) -> Vec<(Bytes, Bytes)> {
    match store.scan(space, start, end, limit) {
        Ok(rows) => rows,
        Err(e) => panic!("scan of {start:?}..{end:?} failed: {e}"),
    }
}

/// Run every assertion against a factory that returns a fresh, empty store.
///
/// # Panics
///
/// On the first assertion the store fails, naming which one.
pub fn check<S: Store>(mut fresh: impl FnMut() -> S) {
    a_committed_key_reads_back(&mut fresh);
    a_delete_removes_a_key(&mut fresh);
    the_applied_index_moves_with_the_batch(&mut fresh);
    the_applied_index_never_goes_backwards(&mut fresh);
    an_empty_batch_still_moves_the_index(&mut fresh);
    the_namespaces_do_not_see_each_other(&mut fresh);
    a_scan_is_ascending_and_half_open(&mut fresh);
    a_scan_respects_its_limit(&mut fresh);
    later_mutations_in_a_batch_win(&mut fresh);
}

fn a_committed_key_reads_back<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    batch.put(Space::User, b"k", b("v"));
    commit(&mut store, 1, batch);
    assert_eq!(
        get(&store, Space::User, b"k"),
        Some(b("v")),
        "a committed key did not read back"
    );
    assert_eq!(get(&store, Space::User, b"absent"), None);
}

fn a_delete_removes_a_key<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    batch.put(Space::User, b"k", b("v"));
    commit(&mut store, 1, batch);

    let mut batch = Batch::new();
    batch.delete(Space::User, b"k");
    commit(&mut store, 2, batch);
    assert_eq!(
        get(&store, Space::User, b"k"),
        None,
        "a deleted key came back"
    );
}

/// The claim the whole seam exists for. After a commit, both the data and the
/// index have moved; there is no ordering in which one is visible and the other
/// is not.
fn the_applied_index_moves_with_the_batch<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    assert_eq!(store.applied(), 0, "a fresh store claims to have applied");

    let mut batch = Batch::new();
    batch.put(Space::User, b"k", b("v"));
    commit(&mut store, 7, batch);

    assert_eq!(store.applied(), 7, "the index did not move with the data");
    assert_eq!(get(&store, Space::User, b"k"), Some(b("v")));
}

/// A commit at or below the current watermark is a replay. Whatever it does to
/// the data, it must not pull the index back — a node that forgot how far it
/// had applied would re-apply everything above it.
fn the_applied_index_never_goes_backwards<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    commit(&mut store, 10, Batch::new());
    commit(&mut store, 3, Batch::new());
    assert_eq!(
        store.applied(),
        10,
        "a commit at a lower index pulled the applied watermark back"
    );
}

/// An entry that changes nothing still has to move the index, or the log would
/// hand it back forever. A no-op entry, a duplicate command, a configuration
/// change: all of them commit an empty batch.
fn an_empty_batch_still_moves_the_index<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    commit(&mut store, 4, Batch::new());
    assert_eq!(store.applied(), 4);
}

/// A client may use any key at all, including one that looks exactly like the
/// state machine's own.
fn the_namespaces_do_not_see_each_other<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    batch.put(Space::User, b"session/1", b("mine"));
    batch.put(Space::Internal, b"session/1", b("the machine's"));
    commit(&mut store, 1, batch);

    assert_eq!(get(&store, Space::User, b"session/1"), Some(b("mine")));
    assert_eq!(
        get(&store, Space::Internal, b"session/1"),
        Some(b("the machine's"))
    );

    let seen = scan(&store, Space::User, None, None, usize::MAX);
    assert_eq!(
        seen.len(),
        1,
        "a scan of one namespace returned the other's keys: {seen:?}"
    );
}

fn a_scan_is_ascending_and_half_open<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    for key in ["a", "b", "c", "d"] {
        batch.put(Space::User, key.as_bytes(), b(key));
    }
    commit(&mut store, 1, batch);

    let keys = |start: Option<&[u8]>, end: Option<&[u8]>| -> Vec<Bytes> {
        scan(&store, Space::User, start, end, usize::MAX)
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    };
    assert_eq!(
        keys(Some(b"b"), Some(b"d")),
        vec![b("b"), b("c")],
        "the start must be included and the end excluded"
    );
    assert_eq!(keys(None, Some(b"b")), vec![b("a")]);
    assert_eq!(keys(Some(b"c"), None), vec![b("c"), b("d")]);
    assert_eq!(keys(None, None).len(), 4);
    assert!(keys(Some(b"z"), None).is_empty());
}

fn a_scan_respects_its_limit<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    for i in 0..20u32 {
        batch.put(Space::User, format!("k{i:02}").as_bytes(), b("v"));
    }
    commit(&mut store, 1, batch);

    let got = scan(&store, Space::User, None, None, 5);
    assert_eq!(got.len(), 5, "a limit of five returned {}", got.len());
    assert_eq!(
        got[0].0,
        b("k00"),
        "a limited scan did not start at the start"
    );
    assert!(
        scan(&store, Space::User, None, None, 0).is_empty(),
        "a limit of zero returned something"
    );
}

/// Two mutations of one key in one batch resolve the way two separate commits
/// would: the later one wins.
fn later_mutations_in_a_batch_win<S: Store>(fresh: &mut impl FnMut() -> S) {
    let mut store = fresh();
    let mut batch = Batch::new();
    batch.put(Space::User, b"k", b("first"));
    batch.put(Space::User, b"k", b("second"));
    batch.put(Space::User, b"gone", b("here"));
    batch.delete(Space::User, b"gone");
    commit(&mut store, 1, batch);

    assert_eq!(
        get(&store, Space::User, b"k"),
        Some(b("second")),
        "an earlier write in the batch won"
    );
    assert_eq!(
        get(&store, Space::User, b"gone"),
        None,
        "a delete later in the batch did not take effect"
    );
}
