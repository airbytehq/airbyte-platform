/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.config.WorkerSourceConfig
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.ContainerIOHandle.Companion.EXIT_CODE_CHECK_EXISTS_FAILURE
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
        every { create(any()) } returns stream
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
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)

    verify(exactly = 1) { messageMetricsTracker.trackConnectionId(connectionId) }
    verify(exactly = 1) { stream.iterator() }
  }

  @Test
  internal fun testSourceIsFinished() {
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
      )

    source.start(sourceConfig = workerSourceConfig, jobRoot = jobRoot, connectionId = connectionId)
    assertEquals(true, source.isFinished)

    every { iterator.hasNext() } returns true
    assertEquals(false, source.isFinished)

    every { iterator.hasNext() } returns false
    exitValueFile.delete()
    assertEquals(false, source.isFinished)

    every { iterator.hasNext() } returns true
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
      )

    val error = assertThrows(WorkerException::class.java, source::cancel)
    assertEquals("Source has not terminated.  This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }
}
