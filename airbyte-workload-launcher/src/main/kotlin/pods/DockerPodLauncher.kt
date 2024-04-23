package io.airbyte.workload.launcher.pods

import io.airbyte.commons.io.IOs
import io.airbyte.config.ResourceRequirements
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.nio.file.Files
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

data class DockerPodConfig(
  val jobDir: Path,
  val name: String,
  val imageName: String,
  val mutex: String?,
  val envMap: Map<String, String>,
  val fileMap: Map<String, String>,
  val orchestratorReqs: ResourceRequirements?,
)

@Singleton
data class DockerConfig(
  @Value("\${airbyte.docker.network}") val dockerNetwork: String,
  @Value("\${airbyte.docker.workspace-mount-name}") val workspaceMountName: String,
  @Value("\${airbyte.docker.workspace-mount-path}") val workspaceMountPath: Path,
  @Value("\${airbyte.docker.docker-socket}") val dockerSocket: Path?,
  @Value("\${airbyte.docker.local-mount-name}") val localMountName: String,
  @Value("\${airbyte.docker.local-mount-path}") val localMountPath: Path,
)

@Singleton
class DockerPodLauncher(private val dockerConfig: DockerConfig) {
  /**
   * Checks if the given container exists.
   */
  fun exists(name: String): Boolean {
    val containerIds = find(mapOf("name" to name))
    return containerIds.isNotEmpty()
  }

  /**
   * Lists docker containers matching the given filters.
   *
   * Returns a list of containerIds.
   */
  fun find(filters: Map<String, String>): List<String> {
    val cmd = mutableListOf("docker", "ps", "--quiet")
    filters.forEach { (k, v) ->
      cmd.add("--filter")
      cmd.add("$k=$v")
    }

    val proc = run(cmd)
    return proc.inputReader().readLines()
  }

  /**
   * Kill containers matching the given mutex key.
   *
   * Returns how many containers matched the mutex key.
   */
  fun kill(mutex: String): Int {
    val containers = find(mapOf("label" to "mutex=$mutex"))
    if (containers.isNotEmpty()) {
      val cmd = listOf("docker", "kill", *containers.toTypedArray())
      val proc = run(cmd)
      logger.info { proc.inputReader().readLines() }
    }
    return containers.size
  }

  /**
   * Launch a pods.
   */
  fun launch(podConfig: DockerPodConfig) {
    val jobWorkspace = dockerConfig.workspaceMountPath.resolve(podConfig.jobDir)

    val cmd = mutableListOf("docker", "run", "--rm", "--init", "-i", "-d")

    cmd.addOption("--name", podConfig.name)
    cmd.addOption("--network", dockerConfig.dockerNetwork)

    cmd.addOption("-w", jobWorkspace.toString())

    cmd.addOption("-v", "${dockerConfig.workspaceMountName}:${dockerConfig.workspaceMountPath}")
    cmd.addOption("-v", "${dockerConfig.localMountName}:${dockerConfig.localMountPath}")

    // mount docker socket so that the orchestrator can start pods
    // the ':' syntax specifies the volume on the local instance to mount to the container
    // e.g. <src-dir-on-local>:<dest-dir-on-container>. Not be confused with Micronaut's
    // default value syntax.
    dockerConfig.dockerSocket?.let {
      cmd.addOption("-v", "$it:/var/run/docker.sock")
    }

    podConfig.mutex?.let {
      cmd.addOption("--label", "mutex=$it")
    }

    copyFiles(jobWorkspace, podConfig.fileMap)

    // add env
    cmd.addOption("--env", "AIRBYTE_CONFIG_DIR=$jobWorkspace")
    writeAndSetEnvFile(cmd, jobWorkspace, podConfig.envMap)
    applyResourceRequirements(cmd, podConfig.orchestratorReqs)

    cmd.add(podConfig.imageName)

    run(cmd)
  }

  private fun copyFiles(
    basePath: Path,
    fileMap: Map<String, String>,
  ) {
    if (!basePath.toFile().exists()) {
      Files.createDirectories(basePath)
    }

    fileMap.forEach { (filename, content) ->
      IOs.writeFile(basePath.resolve(filename), content)
    }
  }

  private fun writeAndSetEnvFile(
    cmd: MutableList<String>,
    basePath: Path,
    envMap: Map<String, String>,
  ) {
    if (!basePath.toFile().exists()) {
      Files.createDirectories(basePath)
    }

    val filePath = basePath.resolve(DOCKER_ENV_FILE_NAME)
    cmd.addOption("--env-file", filePath.toString())
    IOs.writeFile(filePath, envMap.map { (k, v) -> "$k=$v" }.joinToString("\n"))
  }

  private fun applyResourceRequirements(
    cmd: MutableList<String>,
    resourceRequirements: ResourceRequirements?,
  ) {
    resourceRequirements?.apply {
      if (cpuLimit.isNotBlank()) {
        cmd.add("--cpus=$cpuLimit")
      }
      if (memoryRequest.isNotBlank()) {
        cmd.add("--memory-reservation=$memoryRequest")
      }
      if (memoryLimit.isNotBlank()) {
        cmd.add("--memory=$memoryLimit")
      }
    }
  }

  private fun run(cmd: List<String>): Process {
    logger.info { "Running: ${cmd.joinToString(" ")}" }
    val proc = ProcessBuilder(cmd).start()
    // TODO set a limit on how much we wait
    proc.waitFor()
    if (!proc.isAlive && proc.exitValue() != 0) {
      proc.errorReader().lines().forEach { logger.warn { it } }
      throw RuntimeException("failed to run ${cmd.subList(0, 2).joinToString(" ")}")
    }
    return proc
  }

  companion object {
    private val DOCKER_ENV_FILE_NAME = "env-file"
  }

  private fun MutableList<String>.addOption(
    name: String,
    value: String,
  ) {
    this.add(name)
    this.add(value)
  }
}
