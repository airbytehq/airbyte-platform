/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.container.orchestrator.tracker.MessageMetricsTracker
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.EXIT_CODE_CHECK_EXISTS_FAILURE
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.util.UUID
import java.util.stream.Stream

internal class LocalContainerAirbyteDestinationTest {
  private lateinit var destinationTimeoutMonitor: DestinationTimeoutMonitor
  private lateinit var exitValueFile: File
  private lateinit var jobRoot: Path
  private lateinit var containerIOHandle: ContainerIOHandle
  private lateinit var containerLogMdcBuilder: MdcScope.Builder
  private lateinit var message: AirbyteMessage
  private lateinit var messageMetricsTracker: MessageMetricsTracker
  private lateinit var messageWriterFactory: AirbyteMessageBufferedWriterFactory
  private lateinit var randomConnectionId: UUID
  private lateinit var stdErrFile: File
  private lateinit var stdInFile: File
  private lateinit var stdOutFile: File
  private lateinit var stream: Stream<AirbyteMessage>
  private lateinit var streamFactory: AirbyteStreamFactory
  private lateinit var terminationFile: File
  private lateinit var writer: AirbyteMessageBufferedWriter<*>
  private lateinit var workerDestinationConfig: WorkerDestinationConfig

  @BeforeEach
  internal fun setUp() {
    exitValueFile = File.createTempFile("exit", ".txt")
    stdErrFile = File.createTempFile("stdErr", ".txt")
    stdInFile = File.createTempFile("stdIn", ".txt")
    stdOutFile = File.createTempFile("stdOut", ".txt")
    terminationFile = File.createTempFile("termination", ".txt")
    containerIOHandle =
      ContainerIOHandle(
        exitValueFile = exitValueFile,
        errInputStream = stdErrFile.inputStream(),
        inputStream = stdInFile.inputStream(),
        outputStream = stdOutFile.outputStream(),
        terminationFile = terminationFile,
      )
    containerLogMdcBuilder =
      MdcScope
        .Builder()
        .setExtraMdcEntries(LogSource.DESTINATION.toMdc())
    randomConnectionId = UUID.randomUUID()
    destinationTimeoutMonitor = mockk<DestinationTimeoutMonitor>()
    jobRoot = Path.of(".")
    message =
      mockk<AirbyteMessage> {
        every { type } returns AirbyteMessage.Type.RECORD
      }
    messageMetricsTracker =
      mockk<MessageMetricsTracker> {
        every { trackConnectionId(randomConnectionId) } returns Unit
      }
    writer = mockk<AirbyteMessageBufferedWriter<AirbyteMessage>>()
    messageWriterFactory =
      mockk<AirbyteMessageBufferedWriterFactory> {
        every { createWriter(any()) } returns writer
      }
    stream =
      mockk<Stream<AirbyteMessage>> {
        every { filter(any()) } returns this
        every { iterator() } returns listOf(message).iterator()
      }
    streamFactory =
      mockk<AirbyteStreamFactory> {
        every { create(any()) } returns stream
      }
    workerDestinationConfig =
      mockk<WorkerDestinationConfig> {
        every { connectionId } returns randomConnectionId
      }
  }

  @AfterEach
  internal fun tearDown() {
    exitValueFile.delete()
    terminationFile.delete()
  }

  @Test
  internal fun testDestinationStart() {
    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )

    verify(exactly = 1) { messageMetricsTracker.trackConnectionId(randomConnectionId) }
    verify(exactly = 1) { stream.iterator() }
  }

  @Test
  internal fun testDestinationAccept() {
    every { destinationTimeoutMonitor.resetAcceptTimer() } returns Unit
    every { destinationTimeoutMonitor.startAcceptTimer() } returns Unit
    every { messageMetricsTracker.trackConnectionId(randomConnectionId) } returns Unit
    every { messageMetricsTracker.trackDestSent(message.type) } returns Unit
    every { writer.write(message) } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    destination.accept(message = message)

    verify(exactly = 1) { messageMetricsTracker.trackDestSent(any()) }
    verify(exactly = 1) { destinationTimeoutMonitor.startAcceptTimer() }
    verify(exactly = 1) { destinationTimeoutMonitor.resetAcceptTimer() }
    verify(exactly = 1) { writer.write(message) }
  }

  @Test
  internal fun testDestinationNotifyEndOfInput() {
    every { destinationTimeoutMonitor.resetNotifyEndOfInputTimer() } returns Unit
    every { destinationTimeoutMonitor.startNotifyEndOfInputTimer() } returns Unit
    every { writer.flush() } returns Unit
    every { writer.close() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    destination.notifyEndOfInput()

    verify(exactly = 1) { destinationTimeoutMonitor.startNotifyEndOfInputTimer() }
    verify(exactly = 1) { destinationTimeoutMonitor.resetNotifyEndOfInputTimer() }
    verify(exactly = 1) { writer.flush() }
    verify(exactly = 1) { writer.close() }
  }

  @Test
  internal fun testDestinationNotifyEndOfInputAlreadyClosed() {
    every { destinationTimeoutMonitor.resetNotifyEndOfInputTimer() } returns Unit
    every { destinationTimeoutMonitor.startNotifyEndOfInputTimer() } returns Unit
    every { writer.flush() } throws IOException("test")
    every { writer.close() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    assertDoesNotThrow { destination.notifyEndOfInput() }

    verify(exactly = 1) { destinationTimeoutMonitor.startNotifyEndOfInputTimer() }
    verify(exactly = 1) { destinationTimeoutMonitor.resetNotifyEndOfInputTimer() }
  }

  @Test
  internal fun testDestinationIsFinished() {
    val iterator =
      mockk<Iterator<AirbyteMessage>> {
        every { hasNext() } returns false
      }
    every { stream.iterator() } returns iterator

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    assertEquals(true, destination.isFinished)

    every { iterator.hasNext() } returns true
    assertEquals(false, destination.isFinished)

    every { iterator.hasNext() } returns false
    exitValueFile.delete()
    assertEquals(false, destination.isFinished)

    every { iterator.hasNext() } returns true
    assertEquals(false, destination.isFinished)
  }

  @Test
  internal fun testDestinationExitValue() {
    val exitValue = -122

    exitValueFile.writeText(exitValue.toString())

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    assertEquals(exitValue, destination.exitValue)

    exitValueFile.delete()
    val error = assertThrows(IllegalStateException::class.java) { destination.exitValue }
    assertEquals(EXIT_CODE_CHECK_EXISTS_FAILURE, error.message)
  }

  @Test
  internal fun testDestinationAttemptRead() {
    every { message.type } returns AirbyteMessage.Type.RECORD
    every { messageMetricsTracker.trackDestRead(message.type) } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = containerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    val read = destination.attemptRead()
    assertEquals(true, read.isPresent)
    assertEquals(message, read.get())

    every { stream.iterator() } returns listOf<AirbyteMessage>().iterator()
    val read2 = destination.attemptRead()
    assertEquals(false, read2.isPresent)
  }

  @Test
  internal fun testDestinationClose() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns true
        every { getExitCode() } returns exitValue
      }

    every { messageMetricsTracker.flushDestReadCountMetric() } returns Unit
    every { messageMetricsTracker.flushDestSentCountMetric() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.setInputHasEnded(newValue = true)

    assertDoesNotThrow { destination.close() }
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testDestinationCloseInputHasNotEnded() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { getErrInputStream() } returns mockk<InputStream>()
        every { terminate() } returns true
        every { getExitCode() } returns exitValue
        every { getInputStream() } returns mockk<InputStream>()
        every { getOutputStream() } returns mockk<OutputStream>()
      }

    every { destinationTimeoutMonitor.startNotifyEndOfInputTimer() } returns Unit
    every { destinationTimeoutMonitor.resetNotifyEndOfInputTimer() } returns Unit
    every { messageMetricsTracker.flushDestReadCountMetric() } returns Unit
    every { messageMetricsTracker.flushDestSentCountMetric() } returns Unit
    every { writer.close() } returns Unit
    every { writer.flush() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.start(
      destinationConfig = workerDestinationConfig,
      jobRoot = jobRoot,
    )
    destination.setInputHasEnded(newValue = false)

    assertDoesNotThrow { destination.close() }
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testDestinationCloseWithUnexpectedExitValue() {
    val exitValue = -122
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns true
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.flushDestReadCountMetric() } returns Unit
    every { messageMetricsTracker.flushDestSentCountMetric() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.setInputHasEnded(newValue = true)

    val error = assertThrows(WorkerException::class.java, destination::close)
    assertEquals("Destination process exit with code $exitValue. This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testDestinationCloseCancelled() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns false
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.flushDestReadCountMetric() } returns Unit
    every { messageMetricsTracker.flushDestSentCountMetric() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.setInputHasEnded(newValue = true)

    val error = assertThrows(WorkerException::class.java, destination::close)
    assertEquals("Destination has not terminated.  This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  internal fun testDestinationCancel() {
    val exitValue = 0
    val mockedContainerIOHandle =
      mockk<ContainerIOHandle> {
        every { terminate() } returns false
        every { getExitCode() } returns exitValue
      }
    every { messageMetricsTracker.flushDestReadCountMetric() } returns Unit
    every { messageMetricsTracker.flushDestSentCountMetric() } returns Unit

    val destination =
      LocalContainerAirbyteDestination(
        streamFactory = streamFactory,
        messageMetricsTracker = messageMetricsTracker,
        messageWriterFactory = messageWriterFactory,
        containerIOHandle = mockedContainerIOHandle,
        containerLogMdcBuilder = containerLogMdcBuilder,
        destinationTimeoutMonitor = destinationTimeoutMonitor,
      )

    destination.setInputHasEnded(newValue = true)

    val error = assertThrows(WorkerException::class.java, destination::cancel)
    assertEquals("Destination has not terminated.  This warning is normal if the job was cancelled.", error.message)
    verify(exactly = 1) { mockedContainerIOHandle.terminate() }
  }

  @Test
  fun testFlushAfterWrite() {
    val writer = mockk<AirbyteMessageBufferedWriter<AirbyteMessage>>(relaxed = true)
    every { messageWriterFactory.createWriter(any()) } returns writer

    val localContainerAirbyteDestinationWithForcePush =
      LocalContainerAirbyteDestination(
        mockk(relaxed = true),
        mockk(relaxed = true),
        messageWriterFactory,
        mockk(relaxed = true),
        mockk(relaxed = true),
        containerLogMdcBuilder = containerLogMdcBuilder,
        true,
      )

    val message =
      AirbyteMessage()
        .withAdditionalProperty("test", "message")
    val workerDestinationConfig =
      WorkerDestinationConfig()
        .withDestinationId(UUID.randomUUID())

    localContainerAirbyteDestinationWithForcePush.start(workerDestinationConfig, mockk())
    localContainerAirbyteDestinationWithForcePush.acceptWithNoTimeoutMonitor(message)

    verifyOrder {
      writer.write(message)
      writer.flush()
    }
  }
}
