/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.exception.DestinationException
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class DestinationWriterTest {
  private lateinit var mockSource: AirbyteSource
  private lateinit var mockDestination: AirbyteDestination
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockReplicationWorkerHelper: ReplicationWorkerHelper
  private lateinit var mockDestinationQueue: ClosableChannelQueue<AirbyteMessage>

  @BeforeEach
  fun setup() {
    mockSource = mockk(relaxed = true)
    mockDestination = mockk(relaxed = true)
    mockReplicationWorkerState = mockk(relaxed = true)
    mockReplicationWorkerHelper = mockk(relaxed = true)
    mockDestinationQueue = mockk(relaxed = true)

    // By default, we don't abort. This helps in tests for normal flow.
    every { mockReplicationWorkerState.shouldAbort } returns false

    // By default, queue is not closed for receiving.
    every { mockDestinationQueue.isClosedForReceiving() } returns false

    // By default, the source exit value is 0 (typical success code).
    every { mockSource.exitValue } returns 0

    // By default, no additional status messages to send.
    coEvery { mockReplicationWorkerHelper.getStreamStatusToSend(any()) } returns emptyList()
  }

  @Test
  fun `test normal flow - single message`() =
    runTest {
      // Given a single message in the queue
      val airbyteMessage = AirbyteMessage()
      coEvery { mockDestinationQueue.receive() } returns airbyteMessage andThen null
      coEvery { mockDestinationQueue.isClosedForReceiving() } returns false andThen true

      // DestinationWriter under test
      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      // When
      writer.run()

      // Then
      // Verify that accept was called for the single message
      coVerify(exactly = 1) { mockDestination.accept(airbyteMessage) }

      // Because after returning null from the queue, we exit the loop, call getStreamStatusToSend, etc.
      coVerify(exactly = 1) { mockReplicationWorkerHelper.getStreamStatusToSend(0) }

      // Finally, verify that the queue is closed and notifyEndOfInput is called
      verify(exactly = 1) { mockDestination.notifyEndOfInput() }
      verify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test normal flow - multiple messages`() =
    runTest {
      // Given multiple messages
      val msg1 = AirbyteMessage()
      val msg2 = AirbyteMessage()
      coEvery { mockDestinationQueue.receive() } returns msg1 andThen msg2 andThen null
      coEvery { mockDestinationQueue.isClosedForReceiving() } returns false andThen false andThen true

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      // When
      writer.run()

      // Then
      coVerifyOrder {
        mockDestination.accept(msg1)
        mockDestination.accept(msg2)
        mockReplicationWorkerHelper.getStreamStatusToSend(0)
        mockDestination.notifyEndOfInput()
        mockDestinationQueue.close()
      }
    }

  @Test
  fun `test writer stops when shouldAbort is true`() =
    runTest {
      // ShouldAbort is true from the start, so no messages should be consumed
      every { mockReplicationWorkerState.shouldAbort } returns true

      // We'll still provide messages, but they shouldn't be processed
      coEvery { mockDestinationQueue.receive() } returns AirbyteMessage()

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      writer.run()

      // Then
      // Because we abort immediately, no messages are accepted
      coVerify(exactly = 0) { mockDestination.accept(any()) }

      // Final block should still run
      coVerify(exactly = 1) { mockDestination.notifyEndOfInput() }
      coVerify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test writer stops when queue isClosedForReceiving`() =
    runTest {
      // If the queue is closed for receiving from the start, run() should skip the while loop
      every { mockDestinationQueue.isClosedForReceiving() } returns true

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      writer.run()

      // Then
      // Because the queue is closed from the beginning, no messages are consumed
      coVerify(exactly = 0) { mockDestination.accept(any()) }

      // Final block still runs
      coVerify(exactly = 1) { mockDestination.notifyEndOfInput() }
      coVerify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test exception when accepting message throws DestinationException`() =
    runTest {
      // The queue has one message. Accepting it fails.
      val msg = AirbyteMessage()
      coEvery { mockDestinationQueue.receive() } returns msg andThen null
      coEvery { mockDestination.accept(msg) } throws RuntimeException("Simulated failure")

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      // Expect that the DestinationWriter re-wraps in a DestinationException
      val ex =
        assertThrows<DestinationException> {
          writer.run()
        }
      assertTrue(ex.message!!.contains("Destination process message delivery failed"))

      // Final block logic must still be invoked
      coVerify(exactly = 1) { mockDestination.notifyEndOfInput() }
      coVerify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test final block also tries to send status messages`() =
    runTest {
      // No normal messages, so run() stops when queue returns null.
      coEvery { mockDestinationQueue.isClosedForReceiving() } returns true
      // But the replicationWorkerHelper returns some "status messages" after the loop.
      val statusMsg1 = AirbyteMessage()
      val statusMsg2 = AirbyteMessage()
      coEvery { mockReplicationWorkerHelper.getStreamStatusToSend(0) } returns listOf(statusMsg1, statusMsg2)

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      writer.run()

      // Check that both status messages were accepted
      coVerify(exactly = 2) { mockDestination.accept(any()) }

      // And the final block is called
      coVerify(exactly = 1) { mockDestination.notifyEndOfInput() }
      coVerify(exactly = 1) { mockDestinationQueue.close() }
    }

  @Test
  fun `test unexpected exception in run block is rethrown as DestinationException`() =
    runTest {
      // Suppose the replicationWorkerHelper call fails with a random exception
      val dummyException = IllegalStateException("Something went wrong in getStreamStatusToSend")
      coEvery { mockReplicationWorkerHelper.getStreamStatusToSend(any()) } throws dummyException

      coEvery { mockDestinationQueue.isClosedForReceiving() } returns true

      val writer =
        DestinationWriter(
          source = mockSource,
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
          destinationQueue = mockDestinationQueue,
        )

      val ex =
        assertThrows<DestinationException> {
          writer.run()
        }
      assertTrue(ex.message!!.contains("Something went wrong in getStreamStatusToSend"))
      // The cause is the original exception
      assertEquals(dummyException, ex.cause)

      // Final block is still invoked
      coVerify(exactly = 1) { mockDestination.notifyEndOfInput() }
      coVerify(exactly = 1) { mockDestinationQueue.close() }
    }
}
