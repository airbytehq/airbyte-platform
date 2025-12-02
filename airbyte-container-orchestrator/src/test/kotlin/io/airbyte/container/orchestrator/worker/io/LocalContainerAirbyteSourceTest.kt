/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.container.orchestrator.tracker.MessageMetricsTracker
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.EXIT_CODE_CHECK_EXISTS_FAILURE
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteSource.Companion.FALLBACK_EXIT_CODE
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.io.File
import java.nio.file.Path
import java.util.UUID
import java.util.stream.Stream

internal class LocalContainerAirbyteSourceTest {
  private lateinit var connectionId: UUID
  private lateinit var containerLogMdcBuilder: MdcScope.Builder
  private lateinit var exitValueFile: File
  private lateinit var heartbeatMonitor: HeartbeatMonitor
  private lateinit var jobRoot: Path
  private lateinit var containerIOHandle: ContainerIOHandle
  private lateinit var message: AirbyteMessage
  private lateinit var messageMetricsTracker: MessageMetricsTracker
  private lateinit var stdErrFile: File
  private lateinit var stdOutFile: File
  private lateinit var stream: Stream<AirbyteMessage>
  private lateinit var streamFactory: AirbyteStreamFactory
  private lateinit var terminationFile: File
  private lateinit var workerSourceConfig: WorkerSourceConfig

  @BeforeEach
  internal fun setUp() {
    exitValueFile = File.createTempFile("exit", ".txt")
    stdErrFile = File.createTempFile("stdErr", ".txt")
    stdOutFile = File.createTempFile("stdOut", ".txt")
    terminationFile = File.createTempFile("termination", ".txt")
    containerIOHandle =
      ContainerIOHandle(
        exitValueFile = exitValueFile,
        errInputStream = stdErrFile.inputStream(),
        inputStream = stdOutFile.inputStream(),
        outputStream = stdOutFile.outputStream(),
        terminationFile = terminationFile,
      )
    containerLogMdcBuilder =
      MdcScope
        .Builder()
        .setExtraMdcEntriesNonNullable(LogSource.SOURCE.toMdc())
    connectionId = UUID.randomUUID()
    heartbeatMonitor = mockk<HeartbeatMonitor>()
    jobRoot = Path.of(".")
    message = mockk<AirbyteMessage>()
    messageMetricsTracker =
      mockk<MessageMetricsTracker> {
        every { trackConnectionId(connectionId) } returns Unit
      }
    stream =
      mockk<Stream<AirbyteMessage>> {
        every { filter(any()) } returns this
        every { peek(any()) } returns this
        every { iterator() } returns listOf(message).iterator()
      }
    streamFactory =
      mockk<AirbyteStreamFactory> {
        every { create(any(), any()) } returns stream
      }
    workerSourceConfig = mockk<WorkerSourceConfig>()
  }

  @AfterEach
  internal fun tearDown() {
    exitValueFile.delete()
    terminationFile.delete()
  }

  @Test
  internal fun testSourceStart() {
    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)

    verify(exactly = 1) { messageMetricsTracker.trackConnectionId(connectionId) }
    verify(exactly = 1) { stream.iterator() }
  }

  @Test
  internal fun testSourceIsFinishedWithExitCode() {
    // Test normal finish with exit code file present
    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns false
      }
    every { stream.iterator() } returns iterator

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)
    assertEquals(true, source.isFinished)
  }

  @Test
  internal fun testSourceIsNotFinishedWhileHasMessages() {
    // Test that source is not finished while there are still messages
    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns true
      }
    every { stream.iterator() } returns iterator

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)
    assertEquals(false, source.isFinished)
  }

  @Test
  internal fun testSourceExitValue() {
    val exitValue = -122

    exitValueFile.writeText(exitValue.toString())

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    assertEquals(exitValue, source.exitValue)

    exitValueFile.delete()
    val error = assertThrows(IllegalStateException::class.java) { source.exitValue }
    assertEquals(EXIT_CODE_CHECK_EXISTS_FAILURE, error.message)
  }

  @Test
  internal fun testSourceAttemptRead() {
    every { message.type } returns AirbyteMessage.Type.RECORD
    every { messageMetricsTracker.trackSourceRead(message.type) } returns Unit

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)
    val read = source.attemptRead()
    assertEquals(true, read.isPresent)
    assertEquals(message, read.get())

    every { stream.iterator() } returns listOf<AirbyteMessage>().iterator()
    val read2 = source.attemptRead()
    assertEquals(false, read2.isPresent)
  }

  @Test
  internal fun testSourceClose() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns true
        every { exitCodeExists() } returns true
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.trackConnectionId(connectionId) } returns Unit
    every { messageMetricsTracker.flushSourceReadCountMetric() } returns Unit

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    assertDoesNotThrow { source.close() }
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testSourceCloseWithUnexpectedExitValue() {
    val exitValue = -122
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns true
        every { exitCodeExists() } returns true
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.trackConnectionId(connectionId) } returns Unit
    every { messageMetricsTracker.flushSourceReadCountMetric() } returns Unit

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    val error = assertThrows(WorkerException::class.java, source::close)
    assertEquals("Source process exit with code $exitValue. This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testSourceCloseCancelled() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns false
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.trackConnectionId(connectionId) } returns Unit
    every { messageMetricsTracker.flushSourceReadCountMetric() } returns Unit

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    val error = assertThrows(WorkerException::class.java, source::close)
    assertEquals("Source has not terminated.  This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testSourceCancel() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns false
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.trackConnectionId(connectionId) } returns Unit
    every { messageMetricsTracker.flushSourceReadCountMetric() } returns Unit

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    val error = assertThrows(WorkerException::class.java, source::cancel)
    assertEquals("Source has not terminated.  This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testSourceIsFinishedWhenPipeClosedWithoutExitCodeFile() {
    // Simulate OOM scenario: iterator returns no more messages but no exit code file exists
    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns false
      }
    every { stream.iterator() } returns iterator

    // Delete exit code file to simulate OOM where exit code is never written
    exitValueFile.delete()

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        // Use short timeout for test to avoid waiting 30 seconds
        exitCodeWaitSeconds = 1L,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)

    // isFinished should return true even without exit code file when pipe is closed
    assertEquals(true, source.isFinished)

    // exitValue should return the fallback exit code after waiting for the timeout
    assertEquals(FALLBACK_EXIT_CODE, source.exitValue)
  }

  @Test
  internal fun testSourceExitValueWithExitCodeFileWhenPipeClosed() {
    // When exit code file exists, it should be used even if pipe is closed
    val expectedExitCode = 137 // OOM kill signal

    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns false
      }
    every { stream.iterator() } returns iterator

    exitValueFile.writeText(expectedExitCode.toString())

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)

    // isFinished should return true
    assertEquals(true, source.isFinished)

    // exitValue should return the actual exit code from file
    assertEquals(expectedExitCode, source.exitValue)
  }

  @Test
  internal fun testSourceExitValueWaitsForExitCodeFile() {
    // Test that exitValue waits for exit code file to appear (race condition handling)
    val expectedExitCode = 0

    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns false
      }
    every { stream.iterator() } returns iterator

    // Delete exit code file initially
    exitValueFile.delete()

    val source =
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        messageMetricsTracker = messageMetricsTracker,
        streamFactory = streamFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        // Use enough time for our delayed write
        exitCodeWaitSeconds = 5L,
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)

    // isFinished should return true (pipe is closed)
    assertEquals(true, source.isFinished)

    // Simulate exit code file being written after a short delay (race condition)
    Thread {
      Thread.sleep(500)
      exitValueFile.writeText(expectedExitCode.toString())
    }.start()

    // exitValue should wait and find the exit code file
    assertEquals(expectedExitCode, source.exitValue)
  }
}
