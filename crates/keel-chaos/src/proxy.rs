//! A userland partition proxy.
//!
//! Every peer connection goes through it, and it can be told to drop traffic in
//! one direction or both. That is what makes a partition *asymmetric*, which is
//! the shape that finds bugs: a node that can send but not receive still hears
//! nothing and campaigns, while the leader it is talking past sees its
//! heartbeats acknowledged and stays leader. Symmetric partitions are the easy
//! case and the one everybody tests.
//!
//! **Why a proxy rather than a firewall rule.** `iptables` and `pfctl` need
//! root, differ between platforms, and leave state behind when a test is killed.
//! A proxy is a process: kill it and the partition is over. It also makes the
//! partition *observable* — the proxy counts what it carried and what it
//! dropped, so a test can assert that a partition actually cut something rather
//! than assuming the rule took effect.
//!
//! **What it cannot do.** It cannot partition a connection that does not go
//! through it, so every node must be configured to reach its peers via the
//! proxy. A test that pointed one node straight at another would see a
//! partition that silently did not apply — so the proxy refuses to start if it
//! has no route, rather than accepting traffic it cannot control.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Which way traffic is blocked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// Nothing is blocked.
    None,
    /// Traffic from the client side to the server side is dropped; the reverse
    /// still flows. This is the asymmetric case.
    Forward,
    /// The reverse.
    Backward,
    /// Both directions.
    Both,
}

impl Cut {
    fn blocks_forward(self) -> bool {
        matches!(self, Cut::Forward | Cut::Both)
    }

    fn blocks_backward(self) -> bool {
        matches!(self, Cut::Backward | Cut::Both)
    }
}

/// What a proxy carried and what it refused.
#[derive(Debug, Default)]
pub struct Counters {
    pub connections: AtomicU64,
    pub bytes_forward: AtomicU64,
    pub bytes_backward: AtomicU64,
    /// Connections refused outright because the link was cut when they arrived.
    pub refused: AtomicU64,
    /// Connections torn down because the link was cut while they were open.
    pub severed: AtomicU64,
}

impl Counters {
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.connections.load(Ordering::SeqCst),
            self.bytes_forward.load(Ordering::SeqCst),
            self.bytes_backward.load(Ordering::SeqCst),
            self.refused.load(Ordering::SeqCst),
            self.severed.load(Ordering::SeqCst),
        )
    }
}

/// One link: a listening address that forwards to a target.
pub struct Link {
    pub name: String,
    pub listen: SocketAddr,
    pub target: SocketAddr,
    cut: Arc<Mutex<Cut>>,
    pub counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

impl Link {
    /// Start forwarding. Returns once the listener is bound, so a caller can
    /// connect immediately without racing the thread.
    pub fn start(name: &str, listen: SocketAddr, target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(listen)?;
        let bound = listener.local_addr()?;
        let cut = Arc::new(Mutex::new(Cut::None));
        let counters = Arc::new(Counters::default());
        let stop = Arc::new(AtomicBool::new(false));

        {
            let cut = Arc::clone(&cut);
            let counters = Arc::clone(&counters);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || accept_loop(listener, target, cut, counters, stop));
        }

        Ok(Self {
            name: name.to_string(),
            listen: bound,
            target,
            cut,
            counters,
            stop,
        })
    }

    /// Cut, heal, or change direction.
    ///
    /// Takes effect on connections already open as well as new ones: a
    /// partition that only affected future connections would be healed by any
    /// peer that happened to be connected already, which is most of them.
    pub fn set(&self, cut: Cut) {
        if let Ok(mut guard) = self.cut.lock() {
            *guard = cut;
        }
    }

    pub fn cut(&self) -> Cut {
        self.cut.lock().map(|c| *c).unwrap_or(Cut::None)
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop, which is blocking on `accept`. A connection it
        // immediately drops is enough.
        let _ = TcpStream::connect(self.listen);
    }
}

fn accept_loop(
    listener: TcpListener,
    target: SocketAddr,
    cut: Arc<Mutex<Cut>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
) {
    for incoming in listener.incoming() {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let Ok(client) = incoming else { continue };

        // A connection that arrives while the link is cut is refused rather
        // than accepted and starved. A peer learns it cannot connect, which is
        // what a partition looks like from the inside.
        if cut.lock().map(|c| *c != Cut::None).unwrap_or(false) {
            counters.refused.fetch_add(1, Ordering::SeqCst);
            continue;
        }
        let Ok(server) = TcpStream::connect(target) else {
            continue;
        };
        counters.connections.fetch_add(1, Ordering::SeqCst);

        let cut_a = Arc::clone(&cut);
        let cut_b = Arc::clone(&cut);
        let counters_a = Arc::clone(&counters);
        let counters_b = Arc::clone(&counters);
        let (Ok(client_b), Ok(server_b)) = (client.try_clone(), server.try_clone()) else {
            continue;
        };

        std::thread::spawn(move || {
            pump(client, server, cut_a, counters_a, true);
        });
        std::thread::spawn(move || {
            pump(server_b, client_b, cut_b, counters_b, false);
        });
    }
}

/// Copy one direction until the link is cut or the connection ends.
fn pump(
    mut from: TcpStream,
    mut to: TcpStream,
    cut: Arc<Mutex<Cut>>,
    counters: Arc<Counters>,
    forward: bool,
) {
    // A read timeout rather than a blocking read, so a cut is noticed on an
    // idle connection too. A partition that only took effect on the next byte
    // would not partition a cluster that had gone quiet — which is exactly the
    // moment an election timeout is about to fire.
    let _ = from.set_read_timeout(Some(std::time::Duration::from_millis(50)));
    let mut buf = [0u8; 16 * 1024];

    loop {
        let blocked = cut
            .lock()
            .map(|c| {
                if forward {
                    c.blocks_forward()
                } else {
                    c.blocks_backward()
                }
            })
            .unwrap_or(false);
        if blocked {
            // Severed rather than paused. A paused connection delivers a burst
            // of stale messages when the partition heals, which is a different
            // fault from a partition and one no real network produces.
            counters.severed.fetch_add(1, Ordering::SeqCst);
            let _ = from.shutdown(std::net::Shutdown::Both);
            let _ = to.shutdown(std::net::Shutdown::Both);
            return;
        }

        match from.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    return;
                }
                let counter = if forward {
                    &counters.bytes_forward
                } else {
                    &counters.bytes_backward
                };
                counter.fetch_add(n as u64, Ordering::SeqCst);
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => return,
        }
    }
}

/// Every link in a cluster, by name.
#[derive(Default)]
pub struct Mesh {
    links: BTreeMap<String, Link>,
}

impl Mesh {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, link: Link) {
        self.links.insert(link.name.clone(), link);
    }

    pub fn get(&self, name: &str) -> Option<&Link> {
        self.links.get(name)
    }

    pub fn names(&self) -> Vec<String> {
        self.links.keys().cloned().collect()
    }

    /// Heal everything.
    pub fn heal(&self) {
        for link in self.links.values() {
            link.set(Cut::None);
        }
    }

    /// Cut every link whose name matches.
    pub fn cut_matching(&self, cut: Cut, matches: impl Fn(&str) -> bool) {
        for (name, link) in &self.links {
            if matches(name) {
                link.set(cut);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_blocks_the_direction_it_names_and_not_the_other() {
        assert!(!Cut::None.blocks_forward() && !Cut::None.blocks_backward());
        assert!(Cut::Forward.blocks_forward() && !Cut::Forward.blocks_backward());
        assert!(!Cut::Backward.blocks_forward() && Cut::Backward.blocks_backward());
        assert!(Cut::Both.blocks_forward() && Cut::Both.blocks_backward());
    }
}
