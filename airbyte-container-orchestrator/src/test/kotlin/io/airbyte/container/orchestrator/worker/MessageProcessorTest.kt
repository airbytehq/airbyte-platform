/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional

class MessageProcessorTest {
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockReplicationWorkerHelper: ReplicationWorkerHelper
  private lateinit var mockSourceQueue: ClosableChannelQueue<AirbyteMessage>
  private lateinit var mockDestinationQueue: ClosableChannelQueue<AirbyteMessage>

  @BeforeEach
  fun setUp() {
    mockReplicationWorkerState = mockk(relaxed = true)
    mockReplicationWorkerHelper = mockk(relaxed = true)
    mockSourceQueue = mockk(relaxed = true)
    mockDestinationQueue = mockk(relaxed = true)

    // By default, we do not abort
    every { mockReplicationWorkerState.shouldAbort } returns false
    // By default, queues are open
    every { mockSourceQueue.isClosedForReceiving() } returns false
    every { mockDestinationQueue.isClosedForSending() } returns false
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `test normal flow - RECORD and STATE messages are sent to destination`() =
    runTest {
      // Two messages from the source. We'll mark one as RECORD, one as STATE.
      val recordMessage = AirbyteMessage().apply { type = Type.RECORD }
      val stateMessage = AirbyteMessage().apply { type = Type.STATE }

      // The replicationWorkerHelper returns them "as is", i.e. no transformation.
      every { mockReplicationWorkerHelper.processMessageFromSource(recordMessage) } returns Optional.of(recordMessage)
      every { mockReplicationWorkerHelper.processMessageFromSource(stateMessage) } returns Optional.of(stateMessage)

      // The source queue yields these messages, then a null to end the loop
      coEvery { mockSourceQueue.receive() } returns recordMessage andThen stateMessage andThen null
      coEvery { mockSourceQueue.isClosedForReceiving() } returns false andThen false andThen true

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // Verify that we processed both messages
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromSource(recordMessage) }
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromSource(stateMessage) }

      // Because they're RECORD/STATE messages, we send them to the destination queue
      coVerify(exactly = 1) { mockDestinationQueue.send(recordMessage) }
      coVerify(exactly = 1) { mockDestinationQueue.send(stateMessage) }

      // Finally block should close both queues
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test non-record non-state messages are not sent to destination`() =
    runTest {
      // e.g. a TRACE message
      val traceMessage = AirbyteMessage().apply { type = Type.TRACE }

      // The helper returns an Optional of the same message
      every { mockReplicationWorkerHelper.processMessageFromSource(traceMessage) } returns Optional.of(traceMessage)

      // Source queue yields traceMessage, then null
      coEvery { mockSourceQueue.receive() } returns traceMessage andThen null
      coEvery { mockSourceQueue.isClosedForReceiving() } returns false andThen true

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // We invoked processMessageFromSource
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromSource(traceMessage) }
      // But no send to destination queue
      coVerify(exactly = 0) { mockDestinationQueue.send(any()) }

      // Both queues closed
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test empty optional from processMessageFromSource is not sent`() =
    runTest {
      val recordMessage = AirbyteMessage().apply { type = Type.RECORD }
      // The helper returns empty optional
      every { mockReplicationWorkerHelper.processMessageFromSource(recordMessage) } returns Optional.empty()

      coEvery { mockSourceQueue.receive() } returns recordMessage andThen null
      coEvery { mockSourceQueue.isClosedForReceiving() } returns false andThen true

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // We processed the message
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromSource(recordMessage) }
      // Because it returned Optional.empty(), we do not send
      coVerify(exactly = 0) { mockDestinationQueue.send(any()) }

      // Both queues closed
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test null message from source is skipped`() =
    runTest {
      coEvery { mockSourceQueue.receive() } returns null
      coEvery { mockSourceQueue.isClosedForReceiving() } returns false andThen true

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // No messages are processed or sent
      verify(exactly = 0) { mockReplicationWorkerHelper.processMessageFromSource(any()) }
      coVerify(exactly = 0) { mockDestinationQueue.send(any()) }

      // Both queues closed
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test abort stops processing`() =
    runTest {
      // Suppose the replicationWorkerState shouldAbort is false initially, then true
      every { mockReplicationWorkerState.shouldAbort } returnsMany listOf(false, true)

      // We'll return 2 messages from the source. The second message won't be processed if the loop stops in time
      val firstMessage = AirbyteMessage().apply { type = Type.RECORD }
      val secondMessage = AirbyteMessage().apply { type = Type.RECORD }

      coEvery { mockSourceQueue.receive() } returns firstMessage andThen secondMessage
//    coEvery { mockSourceQueue.isClosedForReceiving() } returns false andThen false andThen true

      // The helper returns them as is
      every { mockReplicationWorkerHelper.processMessageFromSource(firstMessage) } returns Optional.of(firstMessage)
      every { mockReplicationWorkerHelper.processMessageFromSource(secondMessage) } returns Optional.of(secondMessage)

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // We process the first message
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromSource(any()) }
      coVerify(exactly = 1) { mockDestinationQueue.send(any()) }

      // Finally, queues are closed
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test sourceQueue closedForReceiving stops processing`() =
    runTest {
      // Suppose the source queue is immediately closed for receiving
      every { mockSourceQueue.isClosedForReceiving() } returns true

      // Even if there's a message, we won't proceed with the loop
      coEvery { mockSourceQueue.receive() } returns AirbyteMessage().apply { type = Type.RECORD }

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // No processing or sending
      verify(exactly = 0) { mockReplicationWorkerHelper.processMessageFromSource(any()) }
      coVerify(exactly = 0) { mockDestinationQueue.send(any()) }

      // Close queues
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test destinationQueue closedForSending stops processing`() =
    runTest {
      // Suppose the destination queue is closed for sending from the beginning
      every { mockDestinationQueue.isClosedForSending() } returns true

      val recordMessage = AirbyteMessage().apply { type = Type.RECORD }
      coEvery { mockSourceQueue.receive() } returns recordMessage andThen null

      val processor =
        MessageProcessor(
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          sourceQueue = mockSourceQueue,
          destinationQueue = mockDestinationQueue,
        )

      processor.run()

      // Because the destination queue is closed, we skip sending
      // We also skip processing the message entirely in the while loop because the condition fails
      verify(exactly = 0) { mockReplicationWorkerHelper.processMessageFromSource(recordMessage) }
      coVerify(exactly = 0) { mockDestinationQueue.send(any()) }

      // Queues closed
      verify(exactly = 1) { mockSourceQueue.close() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }
}
