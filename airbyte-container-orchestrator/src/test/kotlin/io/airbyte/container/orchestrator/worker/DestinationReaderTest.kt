/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.exception.DestinationException
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional

internal class DestinationReaderTest {
  private lateinit var mockDestination: AirbyteDestination
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockReplicationWorkerHelper: ReplicationWorkerHelper

  @BeforeEach
  fun setup() {
    mockDestination = mockk(relaxed = true)
    mockReplicationWorkerState = mockk(relaxed = true)
    mockReplicationWorkerHelper = mockk(relaxed = true)

    // By default, not aborted
    every { mockReplicationWorkerState.shouldAbort } returns false
    // By default, not finished
    every { mockDestination.isFinished } returns false
    // By default, exit value == 0
    every { mockDestination.exitValue } returns 0
  }

  @AfterEach
  fun teardown() {
    clearAllMocks()
  }

  @Test
  fun `test normal flow - messages read, no abort, exit code 0`() =
    runTest {
      // Given multiple messages
      val msg1 = mockk<AirbyteMessage>(relaxed = true)
      val msg2 = mockk<AirbyteMessage>(relaxed = true)

      every { mockDestination.attemptRead() } returnsMany
        listOf(
          Optional.of(msg1),
          Optional.of(msg2),
          Optional.empty(),
        )

      // We'll have the destination become finished after some calls
      every { mockDestination.isFinished } returnsMany
        listOf(
          false,
          false,
          false,
          true,
        )

      val reader =
        DestinationReader(
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
        )

      // When
      reader.run()

      // Then
      // We processed both messages
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromDestination(msg1) }
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromDestination(msg2) }

      // Eventually we exit, exitValue = 0 => endOfDestination
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfDestination() }
      // DestinationReader finished
      // No cancellation, because shouldAbort == false
      verify(exactly = 0) { mockDestination.cancel() }
    }

  @Test
  fun `test no messages leads to yield until finished`() =
    runTest {
      // attemptRead always returns empty. We rely on isFinished = true eventually to break out
      every { mockDestination.attemptRead() } returns Optional.empty()
      every { mockDestination.isFinished } returnsMany listOf(false, true)

      val reader =
        DestinationReader(
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
        )

      reader.run()

      // We never process any message
      verify(exactly = 0) { mockReplicationWorkerHelper.processMessageFromDestination(any()) }

      // exitValue = 0 => endOfDestination
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfDestination() }
    }

  @Test
  fun `test exception in attemptRead is wrapped in DestinationException`() =
    runTest {
      // Suppose attemptRead throws an exception
      val runtimeEx = RuntimeException("simulated read error")
      every { mockDestination.attemptRead() } throws runtimeEx

      val reader =
        DestinationReader(
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
        )

      // The run() should rethrow as DestinationException
      val ex =
        assertThrows<DestinationException> {
          reader.run()
        }
      assertTrue(ex.message!!.contains("Destination process read attempt failed"))
      assertEquals(runtimeEx, ex.cause)

      // We never reach endOfDestination
      verify(exactly = 0) { mockReplicationWorkerHelper.endOfDestination() }
    }

  @Test
  fun `test exitValue non-zero throws DestinationException`() =
    runTest {
      every { mockDestination.isFinished } returns true
      every { mockDestination.exitValue } returns 1 // non-zero

      val reader =
        DestinationReader(
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
        )

      val ex =
        assertThrows<DestinationException> {
          reader.run()
        }
      assertTrue(ex.message!!.contains("Destination process exited with non-zero exit code 1"))

      // No endOfDestination
      verify(exactly = 0) { mockReplicationWorkerHelper.endOfDestination() }
    }

  @Test
  fun `test shouldAbort leads to destination cancel`() =
    runTest {
      // Suppose after one iteration, shouldAbort is set to true
      every { mockDestination.attemptRead() } returns Optional.of(mockk(relaxed = true))
      every { mockReplicationWorkerState.shouldAbort } returnsMany listOf(false, true)

      // We'll say the destination never finishes on its own
      every { mockDestination.isFinished } returns false

      val reader =
        DestinationReader(
          destination = mockDestination,
          replicationWorkerState = mockReplicationWorkerState,
          replicationWorkerHelper = mockReplicationWorkerHelper,
        )

      reader.run()

      // We processed the message once
      verify(exactly = 1) { mockReplicationWorkerHelper.processMessageFromDestination(any()) }
      // Then we abort => destination.cancel()
      verify(exactly = 1) { mockDestination.cancel() }

      // exitValue = 0 => endOfDestination
      verify(exactly = 1) { mockReplicationWorkerHelper.endOfDestination() }
    }
}
