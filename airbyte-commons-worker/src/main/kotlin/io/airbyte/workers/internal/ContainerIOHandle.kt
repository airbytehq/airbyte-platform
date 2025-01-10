/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.workers.pod.FileConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.tools.ant.util.NullOutputStream
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
import java.util.concurrent.CompletableFuture
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
      val nullPipe = NullOutputStream.INSTANCE
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

  fun getErrInputStream(): InputStream {
    return errInputStream
  }

  fun getInputStream(): InputStream {
    return inputStream
  }

  fun getOutputStream(): OutputStream {
    return outputStream
  }

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
    return try {
      terminationFile.writeText(TERMINATION_FILE_BODY)
      val future =
        CompletableFuture.supplyAsync {
          return@supplyAsync watchForExitFile()
        }
      logger.debug { "Waiting for $timeout $timeUnit for exit value..." }
      return future.get(timeout, timeUnit)
    } catch (e: Exception) {
      logger.warn(e) { "Failed to wait for exit value file $exitValueFile to be found." }
      false
    }
  }

  private fun watchForExitFile(): Boolean {
    val watchService = FileSystems.getDefault().newWatchService()
    watchService.use {
      var found = false
      try {
        // register to watch for exit file creation
        exitValueFile.parentFile.toPath().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
        // now check if the file already exists to avoid a race
        found = exitCodeExists()
        while (!found) {
          val watchKey = watchService.take()
          found = watchKey.pollEvents().find {
            (it.context() as Path).endsWith(exitValueFile.name)
          } != null
        }
      } catch (e: Exception) {
        logger.warn(e) { "Exit value file $exitValueFile could not be found." }
      }
      return found
    }
  }
}
