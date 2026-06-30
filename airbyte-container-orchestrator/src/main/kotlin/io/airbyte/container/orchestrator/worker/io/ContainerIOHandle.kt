/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.workers.pod.FileConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.StandardWatchEventKinds
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

data class ContainerIOHandle(
  private val inputStream: InputStream,
  private val outputStream: OutputStream,
  private val errInputStream: InputStream,
  private val exitValueFile: File,
  private val terminationFile: File,
) {
  companion object {
    const val EXIT_CODE_CHECK_EXISTS_FAILURE = "No exit code found."
    const val EXIT_CODE_CHECK_NOT_EMPTY_FAILURE = "Exit code file empty."
    const val TERMINATION_FILE_BODY = "TERMINATE"

    /**
     * Factory methods because source and dest differ on input stream.
     */
    @JvmStatic
    fun dest(): ContainerIOHandle {
      // The order we instantiate these matters as opening a fifo will block.
      val stdErrPipe =
        Channels.newInputStream(
          FileChannel.open(Path.of(FileConstants.DEST_DIR, FileConstants.STDERR_PIPE_FILE), StandardOpenOption.READ),
        )
      val stdOutPipe =
        Channels.newInputStream(
          FileChannel.open(Path.of(FileConstants.DEST_DIR, FileConstants.STDOUT_PIPE_FILE), StandardOpenOption.READ),
        )
      val stdInPipe = Files.newOutputStream(Path.of(FileConstants.DEST_DIR, FileConstants.STDIN_PIPE_FILE))
      val exitValueFile = Path.of(FileConstants.DEST_DIR, FileConstants.EXIT_CODE_FILE).toFile()
      val terminationFile = Path.of(FileConstants.DEST_DIR, FileConstants.TERMINATION_MARKER_FILE).toFile()

      return ContainerIOHandle(
        inputStream = stdOutPipe,
        outputStream = stdInPipe,
        errInputStream = stdErrPipe,
        exitValueFile = exitValueFile,
        terminationFile = terminationFile,
      )
    }

    private object NullOutputStream : OutputStream() {
      override fun write(b: Int) {}
    }

    @JvmStatic
    fun source(): ContainerIOHandle {
      // The order we instantiate these matters as opening a fifo will block.
      val stdErrPipe =
        Channels.newInputStream(
          FileChannel.open(Path.of(FileConstants.SOURCE_DIR, FileConstants.STDERR_PIPE_FILE), StandardOpenOption.READ),
        )
      val stdOutPipe =
        Channels.newInputStream(
          FileChannel.open(Path.of(FileConstants.SOURCE_DIR, FileConstants.STDOUT_PIPE_FILE), StandardOpenOption.READ),
        )
      val nullPipe = NullOutputStream
      val exitValueFile = Path.of(FileConstants.SOURCE_DIR, FileConstants.EXIT_CODE_FILE).toFile()
      val terminationFile = Path.of(FileConstants.SOURCE_DIR, FileConstants.TERMINATION_MARKER_FILE).toFile()

      return ContainerIOHandle(
        inputStream = stdOutPipe,
        outputStream = nullPipe,
        errInputStream = stdErrPipe,
        exitValueFile = exitValueFile,
        terminationFile = terminationFile,
      )
    }
  }

  fun getErrInputStream(): InputStream = errInputStream

  fun getInputStream(): InputStream = inputStream

  fun getOutputStream(): OutputStream = outputStream

  fun exitCodeExists(): Boolean = exitValueFile.exists()

  fun getExitCode(): Int {
    check(exitCodeExists()) { EXIT_CODE_CHECK_EXISTS_FAILURE }
    check(exitValueFile.readText().isNotEmpty()) { EXIT_CODE_CHECK_NOT_EMPTY_FAILURE }
    return exitValueFile.readText().trim().toInt()
  }

  fun terminate(
    timeout: Long = 1L,
    timeUnit: TimeUnit = TimeUnit.MINUTES,
  ): Boolean {
    // Close streams first to unblock any threads stuck on blocking I/O reads.
    // This is critical for shutdown: reader threads may be blocked in native read0() calls
    // on named pipes (FIFOs), which ignore Thread.interrupt() and coroutine cancellation.
    // Closing the underlying stream causes the blocked read to throw IOException,
    // allowing the reader thread to exit and shutdown to proceed.
    closeStreams()

    return try {
      terminationFile.writeText(TERMINATION_FILE_BODY)
      logger.debug { "Waiting for $timeout $timeUnit for exit value..." }
      watchForExitFile(timeout, timeUnit)
    } catch (e: Exception) {
      logger.warn(e) { "Failed to wait for exit value file $exitValueFile to be found." }
      false
    }
  }

  private fun closeStreams() {
    try {
      inputStream.close()
    } catch (e: Exception) {
      logger.debug(e) { "Exception closing input stream" }
    }
    try {
      errInputStream.close()
    } catch (e: Exception) {
      logger.debug(e) { "Exception closing error input stream" }
    }
    try {
      outputStream.close()
    } catch (e: Exception) {
      logger.debug(e) { "Exception closing output stream" }
    }
  }

  fun waitForExitCode(
    timeout: Long = 1L,
    timeUnit: TimeUnit = TimeUnit.MINUTES,
  ): Boolean =
    try {
      logger.debug { "Waiting gracefully for $timeout $timeUnit for exit value..." }
      watchForExitFile(timeout, timeUnit)
    } catch (e: Exception) {
      logger.debug(e) { "Graceful wait for exit value file $exitValueFile timed out or failed." }
      false
    }

  private fun watchForExitFile(
    timeout: Long,
    timeUnit: TimeUnit,
  ): Boolean {
    val watchService = FileSystems.getDefault().newWatchService()
    watchService.use {
      try {
        val deadlineNanos = System.nanoTime() + timeUnit.toNanos(timeout)
        // register to watch for exit file creation
        exitValueFile.parentFile.toPath().register(it, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)
        // now check if the file already exists to avoid a race
        if (exitCodeExists()) {
          return true
        }
        while (true) {
          val remainingNanos = deadlineNanos - System.nanoTime()
          if (remainingNanos <= 0) {
            return false
          }

          val watchKey = it.poll(remainingNanos, TimeUnit.NANOSECONDS) ?: return false
          val found =
            watchKey.pollEvents().any { event ->
              (event.context() as? Path)?.endsWith(exitValueFile.name) == true
            }
          if (found) {
            return true
          }
          if (!watchKey.reset()) {
            return false
          }
        }
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        logger.warn(e) { "Interrupted while waiting for exit value file $exitValueFile to be found." }
        return false
      } catch (e: Exception) {
        logger.warn(e) { "Exit value file $exitValueFile could not be found." }
        return false
      }
    }
  }
}
