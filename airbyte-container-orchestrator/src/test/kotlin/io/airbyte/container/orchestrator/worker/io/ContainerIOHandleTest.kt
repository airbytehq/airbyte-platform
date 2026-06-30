/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.EXIT_CODE_CHECK_EXISTS_FAILURE
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.EXIT_CODE_CHECK_NOT_EMPTY_FAILURE
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.TERMINATION_FILE_BODY
import io.mockk.mockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.lang.Thread.sleep
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

internal class ContainerIOHandleTest {
  private lateinit var exitValueFile: File
  private lateinit var containerIOHandle: ContainerIOHandle
  private lateinit var errInputStream: InputStream
  private lateinit var inputStream: InputStream
  private lateinit var outputStream: OutputStream
  private lateinit var terminationFile: File

  @BeforeEach
  internal fun setUp() {
    exitValueFile = File.createTempFile("exit", ".txt")
    terminationFile = File.createTempFile("termination", ".txt")
    errInputStream = mockk()
    inputStream = mockk()
    outputStream = mockk()

    containerIOHandle =
      ContainerIOHandle(
        exitValueFile = exitValueFile,
        errInputStream = errInputStream,
        inputStream = inputStream,
        outputStream = outputStream,
        terminationFile = terminationFile,
      )
  }

  @AfterEach
  internal fun tearDown() {
    exitValueFile.delete()
    terminationFile.delete()
  }

  @Test
  internal fun testGetErrorInputStream() {
    assertEquals(errInputStream, containerIOHandle.getErrInputStream())
  }

  @Test
  internal fun testGetInputStream() {
    assertEquals(inputStream, containerIOHandle.getInputStream())
  }

  @Test
  internal fun testGetOutputStream() {
    assertEquals(outputStream, containerIOHandle.getOutputStream())
  }

  @Test
  internal fun testGetErrorCode() {
    // File exists but is empty
    val emptyFileError = assertThrows(IllegalStateException::class.java, containerIOHandle::getExitCode)
    assertEquals(EXIT_CODE_CHECK_NOT_EMPTY_FAILURE, emptyFileError.message)

    // File exists and contains an exit value
    val exitCode = -122
    exitValueFile.writeText(exitCode.toString())
    assertEquals(exitCode, containerIOHandle.getExitCode())

    // File does not exist
    exitValueFile.delete()
    val notExistError = assertThrows(IllegalStateException::class.java, containerIOHandle::getExitCode)
    assertEquals(EXIT_CODE_CHECK_EXISTS_FAILURE, notExistError.message)
  }

  @Test
  internal fun testTermination() {
    val resultFuture =
      CompletableFuture.supplyAsync {
        return@supplyAsync containerIOHandle.terminate()
      }

    // Wait for termination process to start watching the exit file
    sleep(TimeUnit.SECONDS.toMillis(1))
    val exitCode = -122
    exitValueFile.writeText(exitCode.toString())

    assertEquals(true, resultFuture.get())
    assertEquals(TERMINATION_FILE_BODY, terminationFile.readText())
  }

  @Test
  internal fun testTerminationRaceCondition() {
    val exitCode = -122
    exitValueFile.writeText(exitCode.toString())

    val result = containerIOHandle.terminate()

    assertEquals(true, result)
    assertEquals(TERMINATION_FILE_BODY, terminationFile.readText())
  }

  @Test
  internal fun testTerminationWithFailure() {
    exitValueFile.delete()
    assertEquals(false, containerIOHandle.terminate(timeUnit = TimeUnit.SECONDS))
  }

  @Test
  internal fun testTerminationTimeoutDoesNotLeaveWatchTaskBlocked() {
    exitValueFile.delete()
    assertNoBlockedExitWatchersAdded {
      containerIOHandle.terminate(100, TimeUnit.MILLISECONDS)
    }
  }

  @Test
  internal fun testTerminationDetectsFileCreatedViaMv() {
    exitValueFile.delete()

    val resultFuture =
      CompletableFuture.supplyAsync {
        return@supplyAsync containerIOHandle.terminate()
      }

    sleep(TimeUnit.SECONDS.toMillis(1))

    // Simulate mv (atomic rename) which fires ENTRY_CREATE, not ENTRY_MODIFY
    val tempFile = File(exitValueFile.parent, "TEMP_EXIT_CODE.txt")
    tempFile.writeText("0")
    tempFile.renameTo(exitValueFile)

    assertEquals(true, resultFuture.get(5, TimeUnit.SECONDS))
  }

  @Test
  internal fun testWaitForExitCodeSuccess() {
    exitValueFile.delete()

    val resultFuture =
      CompletableFuture.supplyAsync {
        return@supplyAsync containerIOHandle.waitForExitCode(5, TimeUnit.SECONDS)
      }

    sleep(TimeUnit.SECONDS.toMillis(1))
    exitValueFile.writeText("0")

    assertEquals(true, resultFuture.get(5, TimeUnit.SECONDS))
  }

  @Test
  internal fun testWaitForExitCodeAlreadyExists() {
    exitValueFile.writeText("0")
    val result = containerIOHandle.waitForExitCode(1, TimeUnit.SECONDS)
    assertEquals(true, result)
  }

  @Test
  internal fun testWaitForExitCodeTimeout() {
    exitValueFile.delete()
    val result = containerIOHandle.waitForExitCode(1, TimeUnit.SECONDS)
    assertEquals(false, result)
  }

  @Test
  internal fun testWaitForExitCodeTimeoutDoesNotLeaveWatchTaskBlocked() {
    exitValueFile.delete()
    assertNoBlockedExitWatchersAdded {
      containerIOHandle.waitForExitCode(100, TimeUnit.MILLISECONDS)
    }
  }

  @Test
  internal fun testWaitForExitCodeDetectsFileCreatedViaMv() {
    exitValueFile.delete()

    val resultFuture =
      CompletableFuture.supplyAsync {
        return@supplyAsync containerIOHandle.waitForExitCode(5, TimeUnit.SECONDS)
      }

    sleep(TimeUnit.SECONDS.toMillis(1))

    // Simulate mv (atomic rename) which fires ENTRY_CREATE, not ENTRY_MODIFY
    val tempFile = File(exitValueFile.parent, "TEMP_EXIT_CODE.txt")
    tempFile.writeText("0")
    tempFile.renameTo(exitValueFile)

    assertEquals(true, resultFuture.get(5, TimeUnit.SECONDS))
  }

  private fun assertNoBlockedExitWatchersAdded(action: () -> Boolean) {
    val blockedWatchersBefore = blockedExitFileWatcherCount()

    assertEquals(false, action())
    sleep(100)

    assertEquals(blockedWatchersBefore, blockedExitFileWatcherCount())
  }

  private fun blockedExitFileWatcherCount(): Int =
    Thread
      .getAllStackTraces()
      .values
      .count { stackTrace ->
        stackTrace.any {
          it.className == "io.airbyte.container.orchestrator.worker.io.ContainerIOHandle" &&
            it.methodName == "watchForExitFile"
        }
      }
}
