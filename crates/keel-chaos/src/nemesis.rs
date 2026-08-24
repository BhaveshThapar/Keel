//! Stopping and killing real processes.
//!
//! Two faults, and they are not the same one.
//!
//! `SIGKILL` is a crash: the process is gone, its file descriptors are closed
//! by the kernel, its peers see connections reset, and whatever it had not
//! fsynced is lost. That is the fault the durability argument is about.
//!
//! `SIGSTOP` is a *pause*, and it is the meaner of the two. The process still
//! holds its sockets open, so its peers see a live TCP connection that never
//! answers — no reset, no refusal, just silence, which is what a garbage
//! collection pause, a hypervisor stall, or a machine that has started swapping
//! actually looks like from outside. Then it resumes holding every belief it
//! had before, including "I am the leader", and does so at a moment when the
//! cluster has already elected somebody else. A node that only ever crashes
//! never gets to be wrong about that.
//!
//! Both go to the process group rather than the process, so a node that spawned
//! a helper cannot leave one behind still holding a port.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::ChaosError;

/// One node under supervision.
pub struct Process {
    pub name: String,
    /// The command that started it, kept so a killed node can be restarted with
    /// the same arguments — a restart with different flags is a different node,
    /// and would quietly turn a crash test into a reconfiguration test.
    program: PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
    log: PathBuf,
    child: Option<Child>,
    pub stopped: bool,
    /// Every kill and every stop, counted. A chaos run that reports "no
    /// violations" without saying how many faults it injected has not said
    /// anything.
    pub kills: u64,
    pub stops: u64,
    pub starts: u64,
}

impl Process {
    pub fn new(
        name: &str,
        program: impl Into<PathBuf>,
        args: Vec<String>,
        env: Vec<(String, String)>,
        log: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.to_string(),
            program: program.into(),
            args,
            env,
            log: log.into(),
            child: None,
            stopped: false,
            kills: 0,
            stops: 0,
            starts: 0,
        }
    }

    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            None => false,
            Some(child) => matches!(child.try_wait(), Ok(None)),
        }
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.as_ref().map(|c| c.id() as i32)
    }

    /// Start, or restart after a kill.
    pub fn start(&mut self) -> Result<(), ChaosError> {
        if self.is_running() {
            return Ok(());
        }
        // Appended, not truncated. A node killed and restarted twenty times
        // leaves one log with twenty lifetimes in it, which is the only way to
        // read what it believed each time it came back.
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        let err = out.try_clone()?;

        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        // Its own process group, so a signal aimed at this node cannot reach
        // the chaos driver, and a helper the node spawned cannot outlive it.
        #[allow(unsafe_code)]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: `setpgid(0, 0)` is async-signal-safe and touches nothing
            // the forked child has not already inherited. It runs between fork
            // and exec, where only such calls are permitted.
            unsafe {
                cmd.pre_exec(|| {
                    if libc::setpgid(0, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }

        self.child = Some(cmd.spawn()?);
        self.stopped = false;
        self.starts += 1;
        Ok(())
    }

    fn signal(&self, sig: i32) -> Result<(), ChaosError> {
        let Some(pid) = self.pid() else {
            return Err(ChaosError::NotRunning(self.name.clone()));
        };
        // Negative pid: the whole group.
        // SAFETY: `kill` inspects no memory. A pid that has already exited
        // yields ESRCH, which is handled rather than ignored.
        #[allow(unsafe_code)]
        let rc = unsafe { libc::kill(-pid, sig) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::ESRCH) {
                return Err(ChaosError::NotRunning(self.name.clone()));
            }
            return Err(ChaosError::Io(e));
        }
        Ok(())
    }

    /// Crash it. Returns once the process has actually been reaped, so a
    /// caller that restarts immediately cannot race the old process's grip on
    /// the port.
    pub fn kill(&mut self) -> Result<(), ChaosError> {
        if !self.is_running() {
            return Err(ChaosError::NotRunning(self.name.clone()));
        }
        // A stopped process cannot be reaped: it has to be resumed to die.
        // Without this the wait below hangs forever, which is a chaos driver
        // that deadlocks on its own fault schedule.
        if self.stopped {
            self.signal(libc::SIGCONT)?;
            self.stopped = false;
        }
        self.signal(libc::SIGKILL)?;
        if let Some(child) = self.child.as_mut() {
            let _ = child.wait();
        }
        self.child = None;
        self.kills += 1;
        Ok(())
    }

    /// Pause it. Its sockets stay open and unanswered.
    pub fn stop(&mut self) -> Result<(), ChaosError> {
        if !self.is_running() {
            return Err(ChaosError::NotRunning(self.name.clone()));
        }
        if self.stopped {
            return Ok(());
        }
        self.signal(libc::SIGSTOP)?;
        self.stopped = true;
        self.stops += 1;
        Ok(())
    }

    /// Resume it, still believing whatever it believed.
    pub fn resume(&mut self) -> Result<(), ChaosError> {
        if !self.stopped {
            return Ok(());
        }
        self.signal(libc::SIGCONT)?;
        self.stopped = false;
        Ok(())
    }

    /// Wait for the node to publish its ready file.
    ///
    /// Waiting for the port would be waiting for the wrong thing: the listener
    /// is bound before the log is replayed, so a client that connects on that
    /// signal talks to a node that has not finished recovering.
    pub fn wait_ready(
        &mut self,
        ready: &std::path::Path,
        within: Duration,
    ) -> Result<(), ChaosError> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if ready.exists() {
                return Ok(());
            }
            if !self.is_running() {
                return Err(ChaosError::DiedDuringStartup(self.name.clone()));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(ChaosError::NeverReady(self.name.clone()))
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // A chaos run that panics must not leave three servers holding ports.
        if self.stopped {
            let _ = self.signal(libc::SIGCONT);
        }
        if self.is_running() {
            let _ = self.signal(libc::SIGKILL);
            if let Some(child) = self.child.as_mut() {
                let _ = child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleeper(dir: &std::path::Path, name: &str) -> Process {
        Process::new(
            name,
            "/bin/sh",
            vec!["-c".into(), "while :; do sleep 0.05; done".into()],
            Vec::new(),
            dir.join(format!("{name}.log")),
        )
    }

    #[test]
    fn a_killed_process_is_reaped_and_can_be_restarted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = sleeper(dir.path(), "a");
        p.start().expect("start");
        assert!(p.is_running());

        p.kill().expect("kill");
        assert!(!p.is_running());
        assert_eq!(p.kills, 1);

        p.start().expect("restart");
        assert!(p.is_running());
        assert_eq!(p.starts, 2, "a restart is a second lifetime, not the first");
    }

    /// The deadlock this guards: a stopped process never reports its exit, so
    /// killing one without resuming it first waits forever.
    #[test]
    fn a_stopped_process_can_still_be_killed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = sleeper(dir.path(), "b");
        p.start().expect("start");
        p.stop().expect("stop");
        assert!(p.stopped);

        p.kill().expect("kill a stopped process");
        assert!(!p.is_running());
    }

    #[test]
    fn a_stopped_process_stays_alive_until_it_is_resumed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = sleeper(dir.path(), "c");
        p.start().expect("start");
        p.stop().expect("stop");
        // Still a process: this is a pause, not a crash. Its sockets would
        // still be open.
        assert!(p.is_running());
        p.resume().expect("resume");
        assert!(!p.stopped);
        assert!(p.is_running());
    }

    #[test]
    fn signalling_something_that_is_not_running_is_an_error_rather_than_a_silent_no_op() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = sleeper(dir.path(), "d");
        assert!(matches!(p.kill(), Err(ChaosError::NotRunning(_))));
        assert!(matches!(p.stop(), Err(ChaosError::NotRunning(_))));
    }

    #[test]
    fn a_process_that_never_gets_ready_is_reported_rather_than_waited_on_forever() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = sleeper(dir.path(), "e");
        p.start().expect("start");
        let never = dir.path().join("never-written");
        assert!(matches!(
            p.wait_ready(&never, Duration::from_millis(150)),
            Err(ChaosError::NeverReady(_))
        ));
    }

    /// A node that exits during startup is a different failure from one that is
    /// slow, and waiting the full timeout on it wastes the run's budget.
    #[test]
    fn a_process_that_dies_during_startup_is_noticed_immediately() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut p = Process::new(
            "f",
            "/bin/sh",
            vec!["-c".into(), "exit 3".into()],
            Vec::new(),
            dir.path().join("f.log"),
        );
        p.start().expect("start");
        let never = dir.path().join("never-written");
        let outcome = p.wait_ready(&never, Duration::from_secs(30));
        assert!(matches!(outcome, Err(ChaosError::DiedDuringStartup(_))));
    }
}
