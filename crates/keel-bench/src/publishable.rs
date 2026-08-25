//! The gate itself.
//!
//! A [`Publishable`] cannot be constructed except by [`Publishable::check`],
//! and `check` returns a [`Refusal`] for every condition under which a number
//! would mislead. That is the whole mechanism: the type is the proof, and a
//! function that needs one cannot be called without it.

use keel_log::SyncMode;

use crate::environment::Environment;

/// How much weight a number is allowed to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// A real measurement on hardware that is not the reference platform:
    /// a laptop, a shared runner, a virtual machine whose neighbours are
    /// unknown. Reproducible, honest, and never headlined — the variance
    /// between two runs on such a host is of the same order as the differences
    /// a benchmark is trying to show.
    Exploratory,
    /// Dedicated Linux hardware, stated in full, with nothing else running.
    /// The only tier a headline number may come from.
    Reference,
}

impl Tier {
    pub fn name(self) -> &'static str {
        match self {
            Self::Exploratory => "Exploratory",
            Self::Reference => "Reference",
        }
    }

    /// Whether a number from this tier may be quoted without its qualifier.
    pub fn may_be_headlined(self) -> bool {
        matches!(self, Self::Reference)
    }
}

/// Why a run may not produce a published number.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error(
        "the data directory is on {0}, which is memory: an fsync there returns without \
         doing anything, so this measures a memcpy rather than a durable write"
    )]
    FilesystemIsMemory(String),
    #[error(
        "the filesystem under the data directory could not be identified, so whether \
         its fsync does anything is unknown"
    )]
    FilesystemUnknown,
    #[error(
        "sync mode is {0}, not durable: {1}. A configuration that does not survive \
         power loss may be measured and may not be published as a durability number"
    )]
    SyncNotDurable(&'static str, &'static str),
    #[error(
        "the hardware was not stated. A throughput figure with no CPU, no memory and \
         no filesystem behind it is not reproducible"
    )]
    HardwareNotStated,
    #[error(
        "{0} run(s): a single repetition says nothing about spread, and a number \
         without spread invites a comparison it cannot support"
    )]
    TooFewRuns(usize),
    #[error(
        "the working tree is modified, so the commit this names is not the code that \
         ran. A number that cannot identify what produced it is not reproducible, which \
         is the same failure as an unstated CPU"
    )]
    TreeModified,
    #[error("the commit could not be determined, so nothing here can be attributed")]
    CommitUnknown,
}

/// How many independent repetitions a published number needs.
///
/// Three, following PR-2: medians of three, with the spread stated. Two would
/// let one outlier decide the answer and give no way to notice.
pub const MINIMUM_RUNS: usize = 3;

fn sync_mode_name(mode: SyncMode) -> (&'static str, &'static str) {
    match mode {
        SyncMode::Durable => ("durable", ""),
        SyncMode::Barrier => (
            "barrier",
            "writes are ordered but not made to survive power loss",
        ),
        SyncMode::None => ("none", "writes are neither ordered nor durable"),
    }
}

/// Proof that a run may be published.
///
/// The only way to get one is [`Publishable::check`]. There is deliberately no
/// constructor, no `Default`, and no way to build one field by field, because
/// each of those is a way for a number to reach `results/bench/` without having
/// passed anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publishable {
    environment: Environment,
    tier: Tier,
    sync_mode: SyncMode,
    runs: usize,
}

impl Publishable {
    pub fn check(environment: &Environment, tier: Tier, runs: usize) -> Result<Self, Refusal> {
        Self::check_with_sync(environment, tier, SyncMode::Durable, runs)
    }

    /// The same, for a caller that wants to be explicit about the sync mode.
    pub fn check_with_sync(
        environment: &Environment,
        tier: Tier,
        sync_mode: SyncMode,
        runs: usize,
    ) -> Result<Self, Refusal> {
        // Hardware first: an environment nobody described cannot be checked for
        // anything else either, and reporting "tmpfs" for a host that was never
        // probed would be a confusing way to say "unprobed".
        if !environment.is_stated() {
            return Err(Refusal::HardwareNotStated);
        }
        match &environment.filesystem {
            crate::Filesystem::Memory(name) => {
                return Err(Refusal::FilesystemIsMemory(name.clone()));
            }
            crate::Filesystem::Unknown => return Err(Refusal::FilesystemUnknown),
            crate::Filesystem::Durable(_) => {}
        }
        if environment.commit.is_empty() {
            return Err(Refusal::CommitUnknown);
        }
        if environment.tree_modified {
            return Err(Refusal::TreeModified);
        }
        if sync_mode != SyncMode::Durable {
            let (name, why) = sync_mode_name(sync_mode);
            return Err(Refusal::SyncNotDurable(name, why));
        }
        if runs < MINIMUM_RUNS {
            return Err(Refusal::TooFewRuns(runs));
        }
        Ok(Self {
            environment: environment.clone(),
            tier,
            sync_mode,
            runs,
        })
    }

    pub fn tier(&self) -> Tier {
        self.tier
    }

    pub fn runs(&self) -> usize {
        self.runs
    }

    pub fn environment(&self) -> &Environment {
        &self.environment
    }

    /// The provenance block a published result carries.
    pub fn header(&self) -> String {
        format!(
            "{}\ntier:   {}{}\nsync:   durable (F_FULLSYNC or fdatasync, per platform)\nruns:   {}",
            self.environment.render(),
            self.tier.name(),
            if self.tier.may_be_headlined() {
                ""
            } else {
                " — not a headline number; see BENCH.md for what that means"
            },
            self.runs,
        )
    }
}

/// A run that may be recorded and may not be published, with the reason it may
/// not stamped into it.
///
/// The second door, and it has to exist. An ablation measuring fsync-off
/// throughput is not a mistake, it is the experiment — the point of it is the
/// comparison with fsync on. Without this, the only way to record that arm
/// would be to weaken the gate, and a gate that is weakened once for a good
/// reason is a gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    environment: Environment,
    refusal: Refusal,
    /// Why this run was worth recording anyway.
    purpose: String,
}

impl Admitted {
    /// Record a run that cannot be published, saying what it is for.
    ///
    /// `purpose` is not decoration. It is the difference between "the fsync-off
    /// arm of the durability ablation" and "we could not get the disk to go
    /// faster", and six months later it is the only thing that distinguishes
    /// them.
    pub fn new(environment: &Environment, refusal: Refusal, purpose: impl Into<String>) -> Self {
        Self {
            environment: environment.clone(),
            refusal,
            purpose: purpose.into(),
        }
    }

    pub fn refusal(&self) -> &Refusal {
        &self.refusal
    }

    pub fn header(&self) -> String {
        format!(
            "{}\ntier:   NOT PUBLISHABLE\nwhy:    {}\nrecorded because: {}",
            self.environment.render(),
            self.refusal,
            self.purpose,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Filesystem;

    fn durable_host() -> Environment {
        Environment {
            cpu: "Test CPU".into(),
            cores: 8,
            memory_gib: 16,
            os: "TestOS 1".into(),
            kernel: "1.0".into(),
            arch: "aarch64".into(),
            filesystem: Filesystem::Durable("apfs".into()),
            data_dir: "/data".into(),
            commit: "abc1234".into(),
            tree_modified: false,
            date: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn a_stated_durable_host_with_enough_runs_passes() {
        let p = Publishable::check(&durable_host(), Tier::Exploratory, 3).expect("passes");
        assert_eq!(p.runs(), 3);
        assert!(p.header().contains("Exploratory"));
        assert!(
            p.header().contains("not a headline number"),
            "an Exploratory result must say so in its own header"
        );
    }

    /// The refusal this whole crate exists for.
    #[test]
    fn a_run_on_memory_is_refused() {
        let mut env = durable_host();
        env.filesystem = Filesystem::Memory("tmpfs".into());
        assert_eq!(
            Publishable::check(&env, Tier::Reference, 5),
            Err(Refusal::FilesystemIsMemory("tmpfs".into()))
        );
    }

    #[test]
    fn a_run_without_fsync_is_refused() {
        for mode in [SyncMode::None, SyncMode::Barrier] {
            let outcome = Publishable::check_with_sync(&durable_host(), Tier::Reference, mode, 5);
            assert!(
                matches!(outcome, Err(Refusal::SyncNotDurable(_, _))),
                "{mode:?} produced {outcome:?}"
            );
        }
    }

    /// A number that cannot say which code produced it is not reproducible.
    #[test]
    fn a_modified_tree_and_an_unknown_commit_are_both_refused() {
        let mut env = durable_host();
        env.tree_modified = true;
        assert_eq!(
            Publishable::check(&env, Tier::Exploratory, 3),
            Err(Refusal::TreeModified)
        );

        let mut env = durable_host();
        env.commit = String::new();
        assert_eq!(
            Publishable::check(&env, Tier::Exploratory, 3),
            Err(Refusal::CommitUnknown)
        );
    }

    #[test]
    fn a_host_nobody_described_is_refused() {
        assert_eq!(
            Publishable::check(&Environment::unknown(), Tier::Exploratory, 9),
            Err(Refusal::HardwareNotStated)
        );
    }

    #[test]
    fn a_filesystem_nobody_could_identify_is_refused() {
        let mut env = durable_host();
        env.filesystem = Filesystem::Unknown;
        // `is_stated` already rejects an unknown filesystem, so this arrives as
        // HardwareNotStated — which is the same refusal for the same reason and
        // is checked here so a change to either cannot silently open a door.
        assert!(Publishable::check(&env, Tier::Exploratory, 9).is_err());
    }

    #[test]
    fn one_run_is_refused_and_three_is_the_floor() {
        assert_eq!(
            Publishable::check(&durable_host(), Tier::Exploratory, 1),
            Err(Refusal::TooFewRuns(1))
        );
        assert_eq!(
            Publishable::check(&durable_host(), Tier::Exploratory, 2),
            Err(Refusal::TooFewRuns(2))
        );
        assert!(Publishable::check(&durable_host(), Tier::Exploratory, MINIMUM_RUNS).is_ok());
    }

    /// The other door, and it says what it is.
    #[test]
    fn an_admitted_run_carries_the_reason_it_cannot_be_published() {
        let mut env = durable_host();
        env.filesystem = Filesystem::Memory("tmpfs".into());
        let refusal = Publishable::check(&env, Tier::Exploratory, 3).unwrap_err();
        let admitted = Admitted::new(&env, refusal, "the tmpfs arm of the storage ablation");
        let header = admitted.header();
        assert!(header.contains("NOT PUBLISHABLE"), "{header}");
        assert!(header.contains("memcpy"), "{header}");
        assert!(header.contains("storage ablation"), "{header}");
    }

    /// Only one tier may be quoted without its qualifier, and it is not the one
    /// every number in this repository currently comes from.
    #[test]
    fn only_the_reference_tier_may_be_headlined() {
        assert!(!Tier::Exploratory.may_be_headlined());
        assert!(Tier::Reference.may_be_headlined());
    }
}
