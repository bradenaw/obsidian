use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Display;
use std::io;
use std::io::BufRead;
use std::io::Cursor;
use std::io::Write;
use std::process::Stdio;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;
use tokio::spawn;

const COLOR_ESCAPES: &[&'static str] = &[
    "\x1b[1;33m", // bright yellow
    "\x1b[37m",   // white
    "\x1b[34m",   // blue
    "\x1b[35m",   // magenta
    "\x1b[36m",   // cyan
    "\x1b[1;34m", // bright blue
    "\x1b[1;35m", // bright magenta
    "\x1b[1;36m", // bright cyan
    "\x1b[1;31m", // bright red
    "\x1b[1;32m", // bright green
    "\x1b[1;37m", // bright white
    "\x1b[33m",   // yellow
    "\x1b[31m",   // red
    "\x1b[32m",   // green
    "\x1b[1;30m", // bright black
                  //"\x1b[30m",   // black
];

const RESET_ESCAPE: &'static str = "\x1b[0m";

const UBUNTU_24_04_IMAGE: &str = "docker.io/library/ubuntu@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55";

/// Manages a set of containers, useful for doing fault injection between networked components.
pub(crate) struct Containers {
    label: String,
    main_network_name: String,
    color_idx: usize,
    containers: BTreeSet<String>,
    kill_on_exit: Child,
    longest_name: Arc<AtomicUsize>,
}

impl Containers {
    pub async fn new() -> anyhow::Result<Self> {
        // TODO: flock

        let label = "obsidian_test";

        log::info!("using label={} on all containers and networks", label);

        cleanup_old(&label).await?;

        let main_network_name = format!("{}-internal-net", label);
        run(Command::new("podman").args([
            "network",
            "create",
            // Don't allow access to the outside.
            "--internal",
            "--label",
            &label,
            &main_network_name,
        ]))
        .await?;

        // Slightly hacky: `podman run` runs the containers in the background. Even if the `podman
        // run` process dies, the containers still stick around. That's not the behavior we want,
        // when the test ends (or gets sigtermed etc.) we want it to clean up after itself.
        //
        // The hacky solution is to spawn this process that waits for its own stdin to close (which
        // happens when _we_ die, because we're holding the other end of the file), and then kills
        // the containers with the label we made.
        //
        // There's still a race here where if we die while spawning a container, this reaper
        // process might clean up too early. Oh well.
        let kill_on_exit = Command::new("bash")
            .arg("-c")
            .arg(format!("while read line; do true; done; podman ps --quiet --filter 'label={}' | xargs podman kill", label))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // TODO: need to spawn a network proxy to get through the --internal?

        Ok(Self {
            label: label.to_string(),
            main_network_name,
            color_idx: 0,
            containers: BTreeSet::new(),
            kill_on_exit,
            longest_name: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn new_container(&mut self, name: impl Into<String>) -> ContainerBuilder<'_> {
        ContainerBuilder {
            parent: self,

            name: name.into(),
            image: None,
            env: BTreeMap::new(),
            args: Vec::new(),
            ports: BTreeSet::new(),
            mounts: Vec::new(),
        }
    }

    pub async fn network_isolate(&mut self, _location_prefix: &str) -> anyhow::Result<()> {
        todo!()
    }

    pub fn network_delay(&mut self, _location_prefix: &str, _delay: Duration) {
        todo!()
    }

    pub fn network_loss(&mut self, _location_prefix: &str, _loss: f64) {
        todo!()
    }

    pub fn network_restore(&mut self, _location_prefix: &str) {
        todo!()
    }

    fn next_color_escape(&mut self) -> &'static str {
        let idx = self.color_idx;
        self.color_idx = (self.color_idx + 1) % COLOR_ESCAPES.len();
        COLOR_ESCAPES[idx]
    }
}

pub(crate) struct ContainerBuilder<'a> {
    parent: &'a mut Containers,

    name: String,
    image: Option<String>,
    ports: BTreeSet<u16>,
    env: BTreeMap<String, String>,
    args: Vec<String>,
    mounts: Vec<(String, String, bool)>,
}

impl<'a> ContainerBuilder<'a> {
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    // TODO: need to plumb these through the network proxy
    pub fn port(mut self, port: u16) -> Self {
        self.ports.insert(port);
        self
    }

    pub fn image(mut self, image: impl Into<String>) -> Self {
        self.image = Some(image.into());
        self
    }

    pub fn mount(
        mut self,
        host_dir: impl Into<String>,
        container_dir: impl Into<String>,
        read_only: bool,
    ) -> Self {
        self.mounts
            .push((host_dir.into(), container_dir.into(), read_only));
        self
    }

    pub fn spawn(self) -> anyhow::Result<Container> {
        if self.parent.containers.contains(&self.name) {
            return Err(anyhow!(
                "container with name {:?} already exists",
                self.name
            ));
        }
        self.parent
            .longest_name
            .fetch_max(self.name.len(), std::sync::atomic::Ordering::SeqCst);
        let mut proc = Command::new("podman")
            .args([
                "container",
                "run",
                "--name",
                &self.name,
                "--label",
                &self.parent.label,
                "--rm",
                //"--network=host",
                "--network",
                &self.parent.main_network_name,
            ])
            .args(
                self.ports
                    .iter()
                    .map(|port| format!("--publish={}:{}", port, port)),
            )
            .args(
                self.env
                    .into_iter()
                    .map(|(key, value)| format!("--env={}={}", key, value)),
            )
            .args(
                self.mounts
                    .into_iter()
                    .map(|(host_dir, container_dir, read_only)| {
                        format!(
                            "--mount=type=bind,source={},target={}{}",
                            host_dir,
                            container_dir,
                            if read_only { ",ro" } else { "" }
                        )
                    }),
            )
            .arg(self.image.unwrap_or(UBUNTU_24_04_IMAGE.into()))
            .args(self.args)
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = proc.stdout.take().unwrap();
        let stderr = proc.stderr.take().unwrap();

        let prefix_writer = PrefixWriter {
            color_escape: self.parent.next_color_escape().to_string(),
            prefix: self.name.clone(),
            prefix_width: Arc::clone(&self.parent.longest_name),
        };

        spawn({
            let prefix_writer = prefix_writer.clone();
            async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    prefix_writer.write_line(&mut io::stdout().lock(), &line);
                }
                prefix_writer.write_line(&mut io::stdout().lock(), "<stdout closed>");
            }
        });
        spawn({
            let prefix_writer = prefix_writer.clone();
            async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    prefix_writer.write_line(&mut io::stderr().lock(), &line);
                }
                prefix_writer.write_line(&mut io::stderr().lock(), "<stderr closed>");
            }
        });
        spawn({
            let prefix_writer = prefix_writer.clone();
            async move {
                match proc.wait().await {
                    Ok(exit_status) => {
                        prefix_writer.write_line(
                            &mut io::stderr().lock(),
                            &format!("process exited with code {}", exit_status.code().unwrap()),
                        );
                    }
                    Err(err) => {
                        prefix_writer.write_line(
                            &mut io::stderr().lock(),
                            &format!("error while waiting for process: {:?}", err),
                        );
                    }
                }
            }
        });

        self.parent.containers.insert(self.name.clone());

        Ok(Container { name: self.name })
    }
}

pub(crate) struct Container {
    name: String,
}

impl Container {
    pub async fn pause(&self) -> anyhow::Result<()> {
        run(Command::new("podman").args(["container", "pause", &self.name])).await?;
        Ok(())
    }

    pub async fn unpause(&self) -> anyhow::Result<()> {
        run(Command::new("podman").args(["container", "unpause", &self.name])).await?;
        Ok(())
    }

    /// Run a command inside the container, returning stdout.
    pub async fn exec(
        &self,
        args: impl IntoIterator<Item = &str>,
    ) -> Result<Vec<u8>, SubprocessError> {
        run(Command::new("podman")
            .arg("exec")
            .arg(&self.name)
            .args(args))
        .await
    }

    pub async fn kill(self) -> anyhow::Result<()> {
        run(Command::new("podman").arg("kill").arg(&self.name)).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct PrefixWriter {
    color_escape: String,
    prefix: String,
    prefix_width: Arc<AtomicUsize>,
}

impl PrefixWriter {
    pub fn write_line(&self, out: &mut impl Write, line: &str) {
        let _ = write!(
            out,
            "{}{}{}{} | {}\n",
            self.color_escape,
            self.prefix,
            RESET_ESCAPE,
            " ".repeat(
                self.prefix_width.load(std::sync::atomic::Ordering::SeqCst) - self.prefix.len()
            ),
            line,
        );
    }
}

async fn cleanup_old(label: &str) -> anyhow::Result<()> {
    log::debug!("cleaning up old containers");
    let container_ps_output = run(Command::new("podman").args([
        "container",
        "ps",
        "--quiet", // only names
        "--all",   // also stopped
        "--filter",
        &format!("label={}", label),
    ]))
    .await?;
    for container_name in BufRead::lines(Cursor::new(container_ps_output)) {
        let container_name = container_name.unwrap();
        log::debug!("cleaning up old containers: {}", container_name);
        run(Command::new("podman").args(["container", "rm", "--force", &container_name])).await?;
    }
    log::debug!("cleaning up old container networks");
    let network_ls_output = run(Command::new("podman").args([
        "network",
        "ls",
        "--quiet", // only names
        "--filter",
        &format!("label={}", label),
    ]))
    .await?;
    for network_name in BufRead::lines(Cursor::new(network_ls_output)) {
        let network_name = network_name.unwrap();
        log::debug!("cleaning up old container networks: {}", network_name);
        run(Command::new("podman").args(["network", "rm", &network_name])).await?;
    }

    Ok(())
}

/// Run the given command returning stdout. If the command does not exit with code 0, returns a
/// SubprocessError that holds its stderr.
async fn run(command: &mut Command) -> Result<Vec<u8>, SubprocessError> {
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let output = command.output().await.map_err(|_| SubprocessError {
        cmd_debug: format!("{:?}", command),
        exit_code: 255,
        stderr: "(process failed to spawn or could not retrieve exit status)"
            .as_bytes()
            .to_vec(),
    })?;
    let exit_code = output.status.code().unwrap();
    if exit_code != 0 {
        return Err(SubprocessError {
            cmd_debug: format!("{:?}", command),
            exit_code,
            stderr: output.stderr,
        });
    }
    Ok(output.stdout)
}

#[derive(Debug)]
pub(crate) struct SubprocessError {
    cmd_debug: String,
    pub exit_code: i32,
    pub stderr: Vec<u8>,
}

impl Display for SubprocessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "command {:?} exited with status {}, stderr:\n{}",
            self.cmd_debug,
            self.exit_code,
            String::from_utf8_lossy(&self.stderr),
        )
    }
}

impl Error for SubprocessError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_output() -> anyhow::Result<()> {
        let out = run(&mut Command::new("echo").arg("foo")).await?;

        assert_eq!(out, "foo\n".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn containers() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();
        let mut containers = Containers::new().await?;
        let _node0 = containers
            .new_container("node0".to_string())
            .arg("bash")
            .arg("-c")
            .arg("echo hello world")
            .spawn()?;
        Ok(())
    }
}
