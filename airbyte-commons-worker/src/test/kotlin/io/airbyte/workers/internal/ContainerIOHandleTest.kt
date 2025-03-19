/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.workers.internal.ContainerIOHandle.Companion.EXIT_CODE_CHECK_EXISTS_FAILURE
import io.airbyte.workers.internal.ContainerIOHandle.Companion.EXIT_CODE_CHECK_NOT_EMPTY_FAILURE
import io.airbyte.workers.internal.ContainerIOHandle.Companion.TERMINATION_FILE_BODY
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
}
