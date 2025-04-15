/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.config.PerformanceMetrics
import io.airbyte.config.ReplicationOutput
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.general.BufferConfiguration
import io.airbyte.workers.helper.StreamStatusCompletionTracker
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.mockk.clearAllMocks
import io.mockk.coVerify
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
import java.nio.file.Path

class ReplicationWorkerKTest {
  private lateinit var mockSource: AirbyteSource
  private lateinit var mockDestination: AirbyteDestination
  private lateinit var mockSyncPersistence: SyncPersistence
  private lateinit var mockWorkloadHeartbeatSender: WorkloadHeartbeatSender
  private lateinit var mockRecordSchemaValidator: RecordSchemaValidator
  private lateinit var mockContext: ReplicationWorkerContext

  private lateinit var mockWorkerHelper: ReplicationWorkerHelperK
  private lateinit var mockWorkerState: ReplicationWorkerState
  private lateinit var mockBufferConfig: BufferConfiguration
  private lateinit var mockStreamStatusCompletionTracker: StreamStatusCompletionTracker

  private lateinit var mockReplicationInput: ReplicationInput
  private lateinit var mockPath: Path

  @BeforeEach
  fun setup() {
    // Create all required mocks
    mockSource = mockk(relaxed = true)
    mockDestination = mockk(relaxed = true)
    mockSyncPersistence = mockk(relaxed = true)
    mockWorkloadHeartbeatSender = mockk(relaxed = true)
    mockRecordSchemaValidator = mockk(relaxed = true)
    mockContext = mockk(relaxed = true)
    mockWorkerHelper = mockk(relaxed = true)
    mockWorkerState = mockk(relaxed = true)
    mockBufferConfig = mockk(relaxed = true)
    mockStreamStatusCompletionTracker = mockk(relaxed = true)

    // Setup context fields
    every { mockContext.replicationWorkerHelper } returns mockWorkerHelper
    every { mockContext.replicationWorkerState } returns mockWorkerState
    every { mockContext.bufferConfiguration } returns mockBufferConfig
    every { mockContext.jobId } returns "0"
    every { mockContext.attempt } returns 1
    every { mockContext.streamStatusCompletionTracker } returns mockStreamStatusCompletionTracker

    // Default buffer sizes
    every { mockBufferConfig.sourceMaxBufferSize } returns 100
    every { mockBufferConfig.destinationMaxBufferSize } returns 100

    // For state
    every { mockWorkerState.cancelled } returns false

    // For replication input
    mockReplicationInput = mockk(relaxed = true)

    // For path
    mockPath = mockk(relaxed = true)
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `test run success`() =
    runTest {
      every { mockSource.isFinished } returns true
      every { mockDestination.isFinished } returns true
      // Given
      val expectedOutput = ReplicationOutput()
      every { mockWorkerHelper.getReplicationOutput(any<PerformanceMetrics>()) } returns expectedOutput

      val worker =
        ReplicationWorkerK(
          source = mockSource,
          destination = mockDestination,
          syncPersistence = mockSyncPersistence,
          onReplicationRunning = {},
          workloadHeartbeatSender = mockWorkloadHeartbeatSender,
          recordSchemaValidator = mockRecordSchemaValidator,
          context = mockContext,
        )

      // When
      val result = worker.run(mockReplicationInput, mockPath)

      // Then
      // Confirm normal flow
      // 1) Initialization
      verify(exactly = 1) { mockWorkerHelper.initialize(mockPath) }

      // 2) Start source & destination
      verify(exactly = 1) { mockWorkerHelper.startDestination(eq(mockDestination), eq(mockPath)) }
      verify(exactly = 1) { mockWorkerHelper.startSource(eq(mockSource), eq(mockReplicationInput), eq(mockPath)) }

      // 3) Mark replication running
      verify(exactly = 1) { mockWorkerState.markReplicationRunning(any()) }

      // 4) Heartbeat was started
      coVerify(atLeast = 1) { mockWorkloadHeartbeatSender.sendHeartbeat() }

      // 5) End of replication if not cancelled
      verify(exactly = 1) { mockWorkerHelper.endOfReplication() }

      // 6) Finally, check the output
      assertEquals(expectedOutput, result)

      // 7) Verify resource usage
      verify(exactly = 1) { mockSyncPersistence.close() }
      verify(exactly = 1) { mockDestination.close() }
      verify(exactly = 1) { mockRecordSchemaValidator.close() }
      verify(exactly = 1) { mockSource.close() }
    }

  @Test
  fun `test run - start source or destination fails`() =
    runTest {
      every { mockDestination.isFinished } returns true
      // Suppose starting the source throws an exception
      every {
        mockWorkerHelper.startSource(any(), any(), any())
      } throws RuntimeException("Simulated start source failure")

      val worker =
        ReplicationWorkerK(
          source = mockSource,
          destination = mockDestination,
          syncPersistence = mockSyncPersistence,
          onReplicationRunning = {},
          workloadHeartbeatSender = mockWorkloadHeartbeatSender,
          recordSchemaValidator = mockRecordSchemaValidator,
          context = mockContext,
        )

      // Because we have a top-level try-catch that rethrows as WorkerException, we expect that
      val ex =
        assertThrows<WorkerException> {
          worker.run(mockReplicationInput, mockPath)
        }
      assertTrue(ex.message!!.contains("Sync failed"))

      // Confirm trackFailure was called
      verify { mockWorkerState.trackFailure(any(), any(), any()) }
      verify { mockWorkerState.markFailed() }

      // The resources should still be used/closed (the use { ... } blocks)
      verify(exactly = 1) { mockSyncPersistence.close() }
      verify(exactly = 1) { mockDestination.close() }
      verify(exactly = 1) { mockRecordSchemaValidator.close() }
      verify(exactly = 1) { mockSource.close() }

      // endOfReplication should not be called because we failed
      verify(exactly = 0) { mockWorkerHelper.endOfReplication() }
    }

  @Test
  fun `test run - subcomponent fails after start`() =
    runTest {
      every { mockDestination.isFinished() } throws RuntimeException("Simulated sub-job failure")

      val worker =
        ReplicationWorkerK(
          source = mockSource,
          destination = mockDestination,
          syncPersistence = mockSyncPersistence,
          onReplicationRunning = {},
          workloadHeartbeatSender = mockWorkloadHeartbeatSender,
          recordSchemaValidator = mockRecordSchemaValidator,
          context = mockContext,
        )

      val ex =
        assertThrows<WorkerException> {
          worker.run(mockReplicationInput, mockPath)
        }
      assertTrue(ex.message!!.contains("Sync failed"))
      // Confirm trackFailure was invoked
      verify(exactly = 1) { mockWorkerState.trackFailure(any(), any(), any()) }
      verify(exactly = 1) { mockWorkerState.markFailed() }

      // endOfReplication won't be called due to error
      verify(exactly = 0) { mockWorkerHelper.endOfReplication() }

      // Resources are still closed
      verify(exactly = 1) { mockSyncPersistence.close() }
      verify(exactly = 1) { mockDestination.close() }
      verify(exactly = 1) { mockRecordSchemaValidator.close() }
      verify(exactly = 1) { mockSource.close() }
    }

  @Test
  fun `test run - cancelled prevents endOfReplication`() =
    runTest {
      // Suppose at the end, replicationWorkerState.cancelled is true
      every { mockWorkerState.cancelled } returns true
      every { mockWorkerState.shouldAbort } returns true

      val output = ReplicationOutput()
      every { mockWorkerHelper.getReplicationOutput(any()) } returns output

      val worker =
        ReplicationWorkerK(
          source = mockSource,
          destination = mockDestination,
          syncPersistence = mockSyncPersistence,
          onReplicationRunning = {},
          workloadHeartbeatSender = mockWorkloadHeartbeatSender,
          recordSchemaValidator = mockRecordSchemaValidator,
          context = mockContext,
        )

      val result = worker.run(mockReplicationInput, mockPath)

      // We do not call endOfReplication() because cancelled = true
      verify(exactly = 0) { mockWorkerHelper.endOfReplication() }

      // We do close resources
      verify(exactly = 1) { mockSyncPersistence.close() }
      verify(exactly = 1) { mockDestination.close() }
      verify(exactly = 1) { mockRecordSchemaValidator.close() }
      verify(exactly = 1) { mockSource.close() }

      // The final getReplicationOutput is returned
      assertEquals(output, result)
    }

  @Test
  fun `test run returns correct output`() =
    runTest {
      every { mockSource.isFinished } returns true
      every { mockDestination.isFinished } returns true
      // We specifically want to ensure that the final returned ReplicationOutput
      // is the one from getReplicationOutput, with the given PerformanceMetrics
      val expectedOutput = ReplicationOutput()
      every { mockWorkerHelper.getReplicationOutput(any()) } returns expectedOutput

      val worker =
        ReplicationWorkerK(
          source = mockSource,
          destination = mockDestination,
          syncPersistence = mockSyncPersistence,
          onReplicationRunning = {},
          workloadHeartbeatSender = mockWorkloadHeartbeatSender,
          recordSchemaValidator = mockRecordSchemaValidator,
          context = mockContext,
        )

      val result = worker.run(mockReplicationInput, mockPath)
      assertEquals(expectedOutput, result)
    }
}
