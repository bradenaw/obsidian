use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Display;
use std::io::BufRead;
use std::io::Cursor;
use std::process::Stdio;
use std::time::Duration;

use anyhow::anyhow;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;
use tokio::spawn;

const COLOR_ESCAPES: &[&'static str] = &[
    "\x1b[33m",   // yellow
    "\x1b[34m",   // blue
    "\x1b[35m",   // magenta
    "\x1b[36m",   // cyan
    "\x1b[31m",   // red
    "\x1b[32m",   // green
    "\x1b[1;33m", // bright yellow
    "\x1b[1;34m", // bright blue
    "\x1b[1;35m", // bright magenta
    "\x1b[1;36m", // bright cyan
    "\x1b[1;31m", // bright red
    "\x1b[1;32m", // bright green
    "\x1b[1;30m", // bright black
    "\x1b[1;37m", // bright white
    //"\x1b[30m",   // black
    "\x1b[37m", // white
];

const RESET_ESCAPE: &'static str = "\x1b[0m";

/// Manages a set of containers, useful for doing fault injection between networked components.
pub(crate) struct Containers {
    label: String,
    main_network_name: String,
    color_idx: usize,
    containers: BTreeSet<String>,
    kill_on_exit: Child,
}

impl Containers {
    pub async fn new(label: String) -> anyhow::Result<Self> {
        // TODO: flock

        let network_ls_output = run(Command::new("podman").args([
            "network",
            "ls",
            "--quiet", // only names
            "--filter",
            &format!("label={}", label),
        ]))
        .await?;
        println!("network_ls_output={:?}", network_ls_output);
        for network_name in BufRead::lines(Cursor::new(network_ls_output)) {
            let network_name = network_name.unwrap();
            log::info!("removing old network {}", network_name);
            run(Command::new("podman").args(["network", "rm", &network_name])).await?;
        }
        let container_ps_output = run(Command::new("podman").args([
            "container",
            "ps",
            "--quiet", // only names
            "--filter",
            &format!("label={}", label),
        ]))
        .await?;
        for container_name in BufRead::lines(Cursor::new(container_ps_output)) {
            run(Command::new("podman").args(["container", "rm", &container_name.unwrap()])).await?;
        }

        //run(Command::new("podman").args([
        //    "network",
        //    "create",
        //    // Don't allow access to the outside.
        //    "--internal",
        //    "--label",
        //    &label,
        //    &label, // name
        //]))
        //.await?;

        // Slightly hacky: `podman run` runs the containers in the background. Even if the `podman
        // run` process dies, the containers still stick around. That's not the behavior we want,
        // when the test ends (or gets sigtermed etc.) we want it to clean up after itself.
        //
        // The hacky solution is to spawn this process that waits for its own stdin to close (which
        // happens when we die), and then kills the containers with the label we made.
        //
        // There's still a race here where if we die while spawning a container, this reaper
        // process might clean up too early. Oh well.
        let kill_on_exit = Command::new("bash")
            .arg("-c")
            .arg(format!("while read line; do true; done; podman ps --quiet --filter 'label={}' | xargs podman kill", label.clone()))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // TODO: need to spawn a network proxy to get through the --internal

        Ok(Self {
            label: label.clone(),
            main_network_name: label.clone(),
            color_idx: 0,
            containers: BTreeSet::new(),
            kill_on_exit,
        })
    }

    pub fn new_container(
        &mut self,
        name: String,
        image: impl Into<String>,
        _location: String,
        command: impl Into<String>,
    ) -> ContainerBuilder<'_> {
        ContainerBuilder {
            parent: self,

            name,
            image: image.into(),
            command: command.into(),
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
    image: String,
    command: String,
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

    pub fn mount(
        &'a mut self,
        host_dir: String,
        container_dir: String,
        read_only: bool,
    ) -> &'a mut ContainerBuilder<'a> {
        self.mounts.push((host_dir, container_dir, read_only));
        self
    }

    pub fn spawn(self) -> anyhow::Result<Container> {
        if self.parent.containers.contains(&self.name) {
            return Err(anyhow!(
                "container with name {:?} already exists",
                self.name
            ));
        }
        let mut proc = Command::new("podman")
            .args([
                "container",
                "run",
                "--name",
                &self.name,
                "--label",
                &self.parent.label,
                "--rm",
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
            .arg(self.image)
            .arg(self.command)
            .args(self.args)
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = proc.stdout.take().unwrap();
        let stderr = proc.stderr.take().unwrap();

        let color_escape = self.parent.next_color_escape();
        let name = self.name.clone();
        spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                println!("{}{}{} | {}", color_escape, name, RESET_ESCAPE, line);
            }
        });
        let name = self.name.clone();
        spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("{}{}{} | {}", color_escape, name, RESET_ESCAPE, line);
            }
        });

        self.parent.containers.insert(self.name.clone());

        Ok(Container {
            proc,
            name: self.name,
        })
    }
}

pub(crate) struct Container {
    proc: Child,
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
    Ok(output.stderr)
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
    async fn containers() -> anyhow::Result<()> {
        let _ = pretty_env_logger::try_init();
        let mut containers = Containers::new("containers_own_test".to_string()).await?;
        let _node0 = containers
            .new_container(
                "node0".to_string(),
                "docker.io/ubuntu/noble", //@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55",
                "location".to_string(),
                "bash",
            )
            .arg("-c")
            .arg("sleep 10000".to_string())
            .spawn()?;
        tokio::time::sleep(Duration::from_secs(500)).await;
        Ok(())
    }
}
