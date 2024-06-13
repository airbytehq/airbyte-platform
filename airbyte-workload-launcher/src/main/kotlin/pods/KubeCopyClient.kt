package io.airbyte.workload.launcher.pods

import io.airbyte.commons.io.IOs
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.process.KubePodProcess
import io.fabric8.kubernetes.api.model.Pod
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

@Singleton
class KubeCopyClient(private val metricClient: MetricClient) {
  fun copyFilesToKubeConfigVolumeMain(
    pod: Pod,
    files: Map<String, String>,
  ) {
    for ((fileName, fileContents) in files.entries) {
      val exitCode = copyFileToPod(pod, fileName, fileContents)
      if (exitCode != 0) {
        throw RuntimeException("kubectl cp failed with exit code $exitCode")
      }
    }

    // copy this file last to indicate that the copy has completed
    val successFileExitCode = copyFileToPod(pod, KubePodProcess.SUCCESS_FILE_NAME, "success")

    // NOTE (copied from KubePodProcess): Copying the success indicator file to the init
    // container causes the container to immediately exit, causing the `kubectl cp` command
    // to exit with code 137. This check ensures that an error is not thrown in this case if
    // the init container exits successfully.
    // TODO: Validate the above and try to avoid 137.
    if (successFileExitCode == 137) {
      metricClient.count(OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_COPY_SUCCESS_OOM, 1)
    } else if (successFileExitCode != 0) {
      throw RuntimeException("kubectl cp failed with exit code $successFileExitCode")
    }
  }

  private fun makeTmpFile(
    fileName: String,
    contents: String,
  ): Path {
    return Path.of(IOs.writeFileToRandomTmpDir(fileName, contents))
  }

  private fun copyToPodProc(
    pod: Pod,
    fileName: String,
    localPath: Path,
  ): Process {
    val containerPath = Path.of(KubePodProcess.CONFIG_DIR + "/" + fileName)

    // using kubectl cp directly here, because both fabric and the official kube client APIs have
    // several issues with copying files. See https://github.com/airbytehq/airbyte/issues/8643 for
    // details.
    val command =
      """
      kubectl cp $localPath ${pod.metadata.namespace}/${pod.metadata.name}:$containerPath -c ${KubePodProcess.INIT_CONTAINER_NAME} --retries=3
      """.trimMargin()

    return Runtime.getRuntime().exec(command)
  }

  private fun copyFileToPod(
    pod: Pod,
    fileName: String,
    fileContents: String,
  ): Int {
    var tmpFilePath: Path? = null
    var proc: Process? = null
    try {
      tmpFilePath = makeTmpFile(fileName, fileContents)
      proc = copyToPodProc(pod, fileName, tmpFilePath)

      val exitCode = proc.waitFor()
      if (exitCode != 0) {
        logger.info { "Fail to copy file $fileName to ${pod.metadata.name}." }
        logger.info { proc.inputReader().readLines().joinToString("\n") }
      }
      return exitCode
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: InterruptedException) {
      throw RuntimeException(e)
    } finally {
      tmpFilePath?.toFile()?.delete()
      proc?.destroy()
    }
  }
}
