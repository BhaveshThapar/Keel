# UMIACS measurement handoff

Transfer the repository **after the v3.0.5 tag exists and
`scripts/release-checklist.sh v3.0.5` passes at that tag**. The code is then
finished locally; moving it earlier would produce numbers for an unnamed tree.

## Allocation contract

Use an exclusive Linux compute node with at least ten physical cores and a
local, non-memory-backed filesystem. Keep the repository and benchmark data on
the measured filesystem, do not run other jobs in the allocation, and record
the exact node and filesystem the gate prints. The same-host reference suite
needs Rust 1.85 or newer, Git, curl, Python 3, Go, and one of Apptainer,
Singularity, Podman, or Docker. The Apptainer branch is intentionally marked
untested until its first cluster run.

Do not copy the macOS `target/` directory. Transfer the tagged source (a clone,
Git bundle, or `rsync` excluding `target/`), check out `v3.0.5`, then run:

```sh
git status --short
git describe --exact-match --tags HEAD
scripts/release-checklist.sh v3.0.5
scripts/umiacs-reference.sh /path/on/the/benchmark/device/keel-reference
```

The suite records the write and balanced curves, fsync ablation, phase
breakdown, 400-trial failover distribution, three 1 GiB snapshot runs, and the
same-host etcd comparison. Results land under `results/bench/`; inspect and
commit them in a separate results commit so the release tag remains immutable.

## Cross-node run

The automatic suite is intentionally same-host: its cluster launcher owns local
processes and its network column is loopback. Do not describe those results as
cross-node.

For a multi-host allocation, start one `keel-server` per allocated host with
the same complete `--peer id=host:port` map and a node-local `--dir`. Bind the
peer, client, and admin listeners to the host's routable interface. From the
load-generator host, verify every `/status` endpoint, then drive the externally
managed cluster with:

```sh
target/release/keel-bench run \
  --nodes host1:7101,host2:7101,host3:7101 \
  --mix writes --rate 6400 --clients 64 --depth 32 --secs 30
```

Repeat each offered rate at least three times and retain the full host list,
allocation identifier, CPU, device, filesystem, and network fabric with the
results. A scheduler-specific launcher is not committed because partition,
module, and scratch paths are site/account policy; substituting guesses there
would make the supposedly reproducible command less reliable.
