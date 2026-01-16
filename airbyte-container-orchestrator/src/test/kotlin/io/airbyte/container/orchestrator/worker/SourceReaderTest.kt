/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.bookkeeping.state.MissingStateInjector
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.internal.exception.SourceException
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional

internal class SourceReaderTest {
  private lateinit var mockSource: AirbyteSource
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockStreamStatusCompletionTracker: StreamStatusCompletionTracker
  private lateinit var mockReplicationWorkerHelper: ReplicationWorkerHelper
  private lateinit var mockMessagesFromSourceQueue: ClosableChannelQueue<AirbyteMessage>
  private lateinit var mockMissingStateInjector: MissingStateInjector

  @BeforeEach
  fun setup() {
    mockSource = mockk(relaxed = true)
    mockReplicationWorkerState = mockk(relaxed = true)
    mockStreamStatusCompletionTracker = mockk(relaxed = true)
    mockReplicationWorkerHelper = mockk(relaxed = true)
    mockMessagesFromSourceQueue = mockk(relaxed = true)
    mockMissingStateInjector = mockk(relaxed = true)

    // By default, we do not abort.
    every { mockReplicationWorkerState.shouldAbort } returns false
    // By default, isFinished = false. We'll override in specific tests as needed.
    every { mockSource.isFinished } returns false
    // By default, queue is not closed for sending
    every { mockMessagesFromSourceQueue.isClosedForSending() } returns false
    // By default, exitValue = 0
    every { mockSource.exitValue } returns 0
  }

  @Test
  fun `test normal flow with messages, including a TRACE-STREAM_STATUS`() =
    runTest {
      // Given
      val normalMessage = AirbyteMessage().also { it.type = Type.RECORD } // example record
      val traceMessage =
        AirbyteMessage().also {
          it.type = Type.TRACE
          it.trace =
            AirbyteTraceMessage().apply {
              type = AirbyteTraceMessage.Type.STREAM_STATUS
              streamStatus = AirbyteStreamStatusTraceMessage()
            }
        }

      // The source will return normalMessage first, then traceMessage, then empty (Optional.empty).
      every { mockSource.attemptRead() } returnsMany
        listOf(
          Optional.of(normalMessage),
          Optional.of(traceMessage),
          Optional.empty(),
        )

      // We’ll simulate that after these reads, the source is finished
      every { mockSource.isFinished } returnsMany listOf(false, false, false, true)

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      // When
      sourceReader.run()

      // Then
      // normalMessage and traceMessage were sent to the queue
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(normalMessage) }
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(traceMessage) }

      // The traceMessage is a STREAM_STATUS, so we track it
      verify(exactly = 1) { mockStreamStatusCompletionTracker.track(traceMessage.trace.streamStatus) }

      // Because exitValue == 0, we call endOfSource
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfSource() }

      // Finally block
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test yield when messageOptional is empty`() =
    runTest {
      // We want to see that the loop doesn't break if we get empty messages.
      // The test here just ensures we keep running until isFinished() is true.
      every { mockSource.attemptRead() } returns Optional.empty()
      every { mockSource.isFinished } returnsMany listOf(false, true)

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      sourceReader.run()

      // No messages should have been sent
      coVerify(exactly = 0) { mockMessagesFromSourceQueue.send(any()) }
      // endOfSource is called if exitValue == 0
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfSource() }
      // close the queue
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test source exitValue non-zero throws RuntimeException`() =
    runTest {
      every { mockSource.isFinished } returns true
      // Non-zero exit code
      every { mockSource.exitValue } returns 1

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      val ex =
        assertThrows<SourceException> {
          sourceReader.run()
        }
      // The cause should be SourceException with a message about non-zero exit code
      assertTrue(ex.message!!.contains("non-zero exit code 1"))

      // Verify endOfSource is not called in this scenario
      verify(exactly = 0) { mockReplicationWorkerHelper.endOfSource() }

      // We do expect the queue to close in finally
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test shouldAbort true triggers source cancel and stops loop`() =
    runTest {
      // Suppose after 1 iteration, shouldAbort becomes true
      every { mockSource.attemptRead() } returns Optional.of(AirbyteMessage())
      every { mockReplicationWorkerState.shouldAbort } returnsMany listOf(false, true)

      // We'll say the source never finishes on its own
      every { mockSource.isFinished } returns false

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      sourceReader.run()

      // We read the message once
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(any()) }
      // Then shouldAbort is true, so we cancel the source
      verify(exactly = 1) { mockSource.cancel() }
      // Because the exitValue by default is 0, we do call endOfSource
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfSource() }
      // The queue is closed
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test queue closed for sending stops the loop`() =
    runTest {
      // Suppose the queue is closed for sending from the start
      every { mockMessagesFromSourceQueue.isClosedForSending() } returns true
      // Source is not finished
      every { mockSource.isFinished } returns false

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      sourceReader.run()

      // Because the queue is closed, we never read messages or send them
      coVerify(exactly = 0) { mockSource.attemptRead() }
      coVerify(exactly = 0) { mockMessagesFromSourceQueue.send(any()) }
      // We do eventually reach the end, exitValue == 0 => endOfSource
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfSource() }
      // queue is closed in finally block
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test SourceException in the try block is wrapped in RuntimeException`() =
    runTest {
      // Suppose the isFinished() method or attemptRead() method throws SourceException for some reason
      every { mockSource.isFinished } throws SourceException("Simulated SourceException")

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = null,
        )

      val ex =
        assertThrows<SourceException> {
          sourceReader.run()
        }
      assertTrue(ex.message!!.contains("Simulated SourceException"))

      // The queue should still be closed
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test missing state injector tracks messages and injects states on completion`() =
    runTest {
      // Given
      val recordMessage = AirbyteMessage().also { it.type = Type.RECORD }
      val stateMessage =
        AirbyteMessage().also {
          it.type = Type.STATE
          it.state =
            AirbyteStateMessage()
              .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
              .withStream(AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("default")))
        }

      // Create injected state messages that the injector will return
      val injectedState1 =
        AirbyteMessage().also {
          it.type = Type.STATE
          it.state =
            AirbyteStateMessage()
              .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
              .withStream(AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("extra1")))
        }
      val injectedState2 =
        AirbyteMessage().also {
          it.type = Type.STATE
          it.state =
            AirbyteStateMessage()
              .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
              .withStream(AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("extra2")))
        }
      val injectedStates = listOf(injectedState1, injectedState2)

      // The source will return recordMessage, then stateMessage, then empty (Optional.empty).
      every { mockSource.attemptRead() } returnsMany
        listOf(
          Optional.of(recordMessage),
          Optional.of(stateMessage),
          Optional.empty(),
        )

      // We'll simulate that after these reads, the source is finished
      every { mockSource.isFinished } returnsMany listOf(false, false, false, true)

      // Mock the missing state injector to return injected states
      every { mockMissingStateInjector.getStatesToInject() } returns injectedStates

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = mockMissingStateInjector,
        )

      // When
      sourceReader.run()

      // Then
      // Verify that all messages from source were tracked by the injector
      verify(exactly = 1) { mockMissingStateInjector.trackMessage(recordMessage) }
      verify(exactly = 1) { mockMissingStateInjector.trackMessage(stateMessage) }

      // Verify that getStatesToInject was called when source finished successfully
      verify(exactly = 1) { mockMissingStateInjector.getStatesToInject() }

      // Verify that original messages were sent to queue
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(recordMessage) }
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(stateMessage) }

      // Verify that injected state messages were sent to queue after source completion
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(injectedState1) }
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(injectedState2) }

      // Because exitValue == 0, we call endOfSource
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfSource() }

      // Finally block
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }

  @Test
  fun `test missing state injector not called when source fails`() =
    runTest {
      // Given
      val recordMessage = AirbyteMessage().also { it.type = Type.RECORD }

      every { mockSource.attemptRead() } returns Optional.of(recordMessage)
      every { mockSource.isFinished } returnsMany listOf(false, true)
      // Source exits with error
      every { mockSource.exitValue } returns 1

      val sourceReader =
        SourceReader(
          source = mockSource,
          replicationWorkerState = mockReplicationWorkerState,
          streamStatusCompletionTracker = mockStreamStatusCompletionTracker,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          messagesFromSourceQueue = mockMessagesFromSourceQueue,
          missingStateInjector = mockMissingStateInjector,
        )

      // When
      assertThrows<SourceException> {
        sourceReader.run()
      }

      // Then
      // Verify that message was tracked
      verify(exactly = 1) { mockMissingStateInjector.trackMessage(recordMessage) }

      // Verify that getStatesToInject was NOT called because source failed
      verify(exactly = 0) { mockMissingStateInjector.getStatesToInject() }

      // Verify that original message was sent to queue
      coVerify(exactly = 1) { mockMessagesFromSourceQueue.send(recordMessage) }

      // endOfSource is not called when source fails
      verify(exactly = 0) { mockReplicationWorkerHelper.endOfSource() }

      // Finally block still closes queue
      verify(exactly = 1) { mockMessagesFromSourceQueue.close() }
    }
}
