/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.asyncProfiler

import dev.failsafe.Failsafe
import dev.failsafe.function.CheckedSupplier
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.File
import java.util.Locale

private val logger = KotlinLogging.logger {}

@Singleton
open class ProfilerService(
  private val fileServiceImpl: FileService,
  private val failSafeRetryPoliciesImpl: FailSafeRetryPolicies,
) {
  protected open fun createProcessBuilder(vararg command: String): ProcessBuilder = ProcessBuilder(*command)

  private fun findPidOfJavaProcess(mainClassKeyword: String): Int? {
    val pidRetryPolicy = failSafeRetryPoliciesImpl.pidRetryPolicy(mainClassKeyword)

    return Failsafe.with(pidRetryPolicy).get(
      CheckedSupplier {
        val process = createProcessBuilder("jps", "-l").start()

        process.inputReader().use { reader ->
          reader
            .lineSequence()
            .map { it.split("\\s+".toRegex()) }
            .firstOrNull { parts -> parts.size == 2 && parts[1].contains(mainClassKeyword) }
            ?.first()
            ?.toInt()
        }
      },
    )
  }

  fun runProfiler(
    mainClassKeyword: String,
    eventType: String,
  ): ProfilerThreadManager.ProfilingResult {
    return try {
      val pid =
        findPidOfJavaProcess(mainClassKeyword)
          ?: return ProfilerThreadManager.ProfilingResult(
            ProfilerThreadManager.ProfilerResult.FAILED,
            "Could not find process containing '$mainClassKeyword'",
            null,
          )

      val (osName, archName) = getOsInformation()

      val tempDir = fileServiceImpl.createTempDirectory("asyncprof-")

      val downloadFileName =
        resolveDownloadFileName(osName, archName)
          ?: throw IllegalStateException("Unsupported OS/arch combination: $osName / $archName")

      val archiveFile = File(tempDir, downloadFileName)
      val downloadUrl = "$BASE_URL$downloadFileName"

      fileServiceImpl.downloadFile(downloadUrl, archiveFile)
      fileServiceImpl.extractArchive(archiveFile)

      val scriptFile =
        fileServiceImpl.findProfilerScript(tempDir)
          ?: throw RuntimeException("Could not find profiler.sh or asprof after extraction.")
      scriptFile.setExecutable(true)

      val outputPath = getOutputPath(tempDir, mainClassKeyword, eventType)

      val profilerCmd = profilerCommand(scriptFile, outputPath, eventType, pid)
      logger.info { "Running profiler command: $profilerCmd" }

      val process = createProcessBuilder(*profilerCmd.toTypedArray()).start()

      // Log streams asynchronously:
      logProfilerOutput(process, mainClassKeyword)

      val exitCode = process.waitFor()
      return if (exitCode == 0) {
        if (fileServiceImpl.fileExists(outputPath)) {
          logger.info { "Profiler wrote output to: $outputPath" }
          ProfilerThreadManager.ProfilingResult(
            ProfilerThreadManager.ProfilerResult.SUCCESS,
            "Profiling succeeded! See $outputPath",
            outputPath.toString(),
          )
        } else {
          logger.warn {
            "Profiler finished with exit code 0 but output file not found: $outputPath."
          }
          ProfilerThreadManager.ProfilingResult(
            ProfilerThreadManager.ProfilerResult.FAILED,
            "Profiler ended with exit code 0, but no output file found.",
            null,
          )
        }
      } else {
        ProfilerThreadManager.ProfilingResult(
          ProfilerThreadManager.ProfilerResult.FAILED,
          "Profiling failed with exit code $exitCode",
          null,
        )
      }
    } catch (ex: Exception) {
      logger.error(ex) { "Error while running the profiler" }
      ProfilerThreadManager.ProfilingResult(
        ProfilerThreadManager.ProfilerResult.FAILED,
        "Error: ${ex.message}",
        null,
      )
    }
  }

  private fun profilerCommand(
    scriptFile: File,
    outputPath: String,
    chosenEvent: String,
    pid: Int,
  ): List<String> {
    val baseCommand =
      mutableListOf(
        scriptFile.absolutePath,
        "-f",
        outputPath,
        "-e",
        chosenEvent,
        "-d",
        SECONDS_IN_24_HRS,
      )

    if (chosenEvent == "wall" || chosenEvent == "lock") {
      baseCommand.addAll(
        listOf(
          "-i",
          SAMPLING_INTERVAL,
          "-t",
        ),
      )
    }

    baseCommand.addAll(
      listOf(
        "-o",
        "tree",
        pid.toString(),
      ),
    )

    return baseCommand
  }

  private fun logProfilerOutput(
    process: Process,
    mainClassKeyword: String,
  ) {
    Thread {
      process.inputStream.bufferedReader().lines().forEach { line ->
        logger.info { "Profiler STDOUT> $line" }
      }
    }.apply {
      name = "profiler-STDOUT-thread-$mainClassKeyword"
    }.start()

    Thread {
      process.errorStream.bufferedReader().lines().forEach { line ->
        logger.info { "Profiler STDERR> $line" }
      }
    }.apply {
      name = "profiler-STDERR-thread-$mainClassKeyword"
    }.start()
  }

  private fun getOsInformation(): OsInformation {
    val osName = System.getProperty(OS_NAME).lowercase(Locale.getDefault())
    val archName = System.getProperty(OS_ARCH).lowercase(Locale.getDefault())
    logger.info { "Detected OS: $osName, arch: $archName" }
    return OsInformation(osName, archName)
  }

  private fun getOutputPath(
    tempDir: File,
    mainClassKeyword: String,
    eventType: String,
  ) = File(tempDir, "$mainClassKeyword-$eventType.${fileExtension()}").absolutePath

  private fun fileExtension() = "html"

  private fun resolveDownloadFileName(
    os: String,
    arch: String,
  ): String? =
    when {
      os.contains(LINUX) && (arch.contains(ARCH_X86_64) || arch.contains(ARCH_AMD_64)) ->
        "$ASYNC_PROFILER_FILE_PREFIX$ASYNC_PROFILER_VERSION-linux-x64.tar.gz"
      os.contains(LINUX) && (arch.contains(ARCH_AARCH_64) || arch.contains(ARCH_ARM_64)) ->
        "$ASYNC_PROFILER_FILE_PREFIX$ASYNC_PROFILER_VERSION-linux-arm64.tar.gz"
      os.contains(MAC) ->
        "$ASYNC_PROFILER_FILE_PREFIX$ASYNC_PROFILER_VERSION-macos.zip"
      else ->
        null
    }

  private data class OsInformation(
    val osName: String,
    val archName: String,
  )

  companion object {
    private const val ARCH_ARM_64 = "arm64"
    private const val ARCH_AARCH_64 = "aarch64"
    private const val MAC = "mac"
    private const val ARCH_X86_64 = "x86_64"
    private const val ARCH_AMD_64 = "amd64"
    private const val LINUX = "linux"
    private const val ASYNC_PROFILER_VERSION = "4.0"
    private const val ASYNC_PROFILER_FILE_PREFIX = "async-profiler-"
    private const val BASE_URL = "https://github.com/async-profiler/async-profiler/releases/download/v$ASYNC_PROFILER_VERSION/"
    private const val SECONDS_IN_24_HRS = "86400"
    private const val SAMPLING_INTERVAL = "1ms"

    private const val OS_NAME = "os.name"
    private const val OS_ARCH = "os.arch"
  }
}
