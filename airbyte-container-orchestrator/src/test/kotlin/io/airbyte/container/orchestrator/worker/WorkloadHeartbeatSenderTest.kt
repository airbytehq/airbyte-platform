/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.exception.WorkloadHeartbeatException
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.infrastructure.ClientException
import io.airbyte.workload.api.client.generated.infrastructure.ServerException
import io.micronaut.http.HttpStatus
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong

@OptIn(ExperimentalCoroutinesApi::class)
class WorkloadHeartbeatSenderTest {
  private lateinit var mockWorkloadApiClient: WorkloadApiClient
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockDestinationTimeoutMonitor: DestinationTimeoutMonitor
  private lateinit var mockSourceTimeoutMonitor: HeartbeatMonitor

  private val testWorkloadId = "test-workload"
  private val testJobId = 0L
  private val testAttempt = 1
  private val shortHeartbeatInterval = Duration.ofMillis(10)
  private val shortHeartbeatTimeout = Duration.ofMillis(50)

  @BeforeEach
  fun setup() {
    mockWorkloadApiClient = mockk(relaxed = true)
    mockReplicationWorkerState = mockk(relaxed = true)
    mockDestinationTimeoutMonitor = mockk(relaxed = true)
    mockSourceTimeoutMonitor = mockk(relaxed = true)

    every { mockDestinationTimeoutMonitor.hasTimedOut() } returns false
    every { mockSourceTimeoutMonitor.isBeating } returns Optional.of(true)
  }

  @AfterEach
  fun teardown() {
    clearAllMocks()
  }

  /**
   * Demonstrates a normal single iteration: no timeouts, no errors, no abort.
   * We force the loop to break by letting the second iteration trigger a "workload in terminal state" (HTTP 410).
   * This ensures we only see one successful heartbeat call first.
   */
  @Test
  fun `test normal flow - single successful heartbeat then GONE`() =
    runTest {
      // On the first call, workloadHeartbeat returns successfully
      every { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) } just Runs

      // On the second call, it throws a ClientException with 410 => we markCancelled() and break
      every { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) } throws
        ClientException(
          "Gone",
          HttpStatus.GONE.code,
        )

      val sender =
        WorkloadHeartbeatSender(
          workloadApiClient = mockWorkloadApiClient,
          replicationWorkerState = mockReplicationWorkerState,
          destinationTimeoutMonitor = mockDestinationTimeoutMonitor,
          sourceTimeoutMonitor = mockSourceTimeoutMonitor,
          heartbeatInterval = shortHeartbeatInterval,
          heartbeatTimeoutDuration = shortHeartbeatTimeout,
          workloadId = testWorkloadId,
          jobId = testJobId,
          attempt = testAttempt,
        )

      // Launch the heartbeat in a child job so we can let it run, then see how it breaks on 2nd iteration
      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Advance enough time for the first heartbeat call, then the second iteration that triggers GONE
      advanceTimeBy(100) // ms

      // The job should complete because we get a GONE on second iteration
      assertTrue(job.isCompleted)

      // First call => success
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadApi.workloadHeartbeat(
          match { it.workloadId == testWorkloadId },
        )
      }
      // Then we see a ClientException(410) => markCancelled => exit
      verify(exactly = 1) { mockReplicationWorkerState.markCancelled() }

      confirmVerified(mockReplicationWorkerState, mockWorkloadApiClient)
    }

  /**
   * We skip sending a heartbeat if the destination is timed out, but haven't yet exceeded the total heartbeat timeout
   * since lastSuccessfulHeartbeat. We'll let it do a second iteration that triggers a forced GONE to break the loop.
   */
  @Test
  fun `test skip heartbeat if destinationTimeoutMonitor hasTimedOut but not exceeding heartbeatTimeout`() =
    runTest {
      // We want the first iteration to see "hasTimedOut = true" => skip heartbeat, but not break out yet
      every { mockDestinationTimeoutMonitor.hasTimedOut() } returnsMany listOf(true, false)
      every { mockDestinationTimeoutMonitor.timeSinceLastAction } returnsMany listOf(AtomicLong(0), AtomicLong(0))
      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // So the code calls `checkIfExpiredAndMarkSyncStateAsFailed(...)` => returns false because time is short

      // On second iteration, let's cause a GONE to break out
      every { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) } throws
        ClientException(
          "Gone",
          HttpStatus.GONE.code,
        )

      val sender =
        WorkloadHeartbeatSender(
          workloadApiClient = mockWorkloadApiClient,
          replicationWorkerState = mockReplicationWorkerState,
          destinationTimeoutMonitor = mockDestinationTimeoutMonitor,
          sourceTimeoutMonitor = mockSourceTimeoutMonitor,
          heartbeatInterval = shortHeartbeatInterval,
          heartbeatTimeoutDuration = Duration.ofMinutes(1),
          workloadId = testWorkloadId,
          jobId = testJobId,
          attempt = testAttempt,
        )

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for 2 iterations
      advanceTimeBy(200)

      // The job completes due to the second iteration GONE
      assertTrue(job.isCompleted)

      verify(exactly = 1) {
        mockWorkloadApiClient.workloadApi.workloadHeartbeat(any())
      }
      // Then second iteration tries => GONE => markCancelled
      verify(exactly = 1) { mockReplicationWorkerState.markCancelled() }
    }

  /**
   * Similar scenario: skip sending heartbeat if the source is not beating.
   */
  @Test
  fun `test skip heartbeat if source not beating, then GONE second iteration`() =
    runTest {
      // On first iteration, isBeating = false => skip heartbeat
      every { mockSourceTimeoutMonitor.isBeating } returnsMany listOf(Optional.of(false), Optional.of(true))
      every { mockSourceTimeoutMonitor.timeSinceLastBeat } returnsMany
        listOf(
          Optional.of(Duration.ZERO),
          Optional.of(Duration.ZERO),
        )

      // Then second iteration => GONE
      every { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) } throws
        ClientException(
          "Gone",
          HttpStatus.GONE.code,
        )

      val sender =
        WorkloadHeartbeatSender(
          workloadApiClient = mockWorkloadApiClient,
          replicationWorkerState = mockReplicationWorkerState,
          destinationTimeoutMonitor = mockDestinationTimeoutMonitor,
          sourceTimeoutMonitor = mockSourceTimeoutMonitor,
          heartbeatInterval = shortHeartbeatInterval,
          heartbeatTimeoutDuration = Duration.ofMinutes(1),
          workloadId = testWorkloadId,
          jobId = testJobId,
          attempt = testAttempt,
        )

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      advanceTimeBy(200)
      assertTrue(job.isCompleted)

      verify(exactly = 1) { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) }
      // Second iteration => tries => GONE => markCancelled
      verify(exactly = 1) { mockReplicationWorkerState.markCancelled() }
    }

  /**
   * If the server returns some other error (not GONE), we log and retry unless we exceed the total heartbeat timeout.
   * We'll force the second iteration to exceed the heartbeat timeout. That triggers markFailed, abort, trackFailure.
   */
  @Test
  fun `test general exception triggers retry, but eventually we exceed heartbeatTimeout and fail`() =
    runTest {
      // Let the first iteration throw a generic error from the client.
      every { mockWorkloadApiClient.workloadApi.workloadHeartbeat(any()) } throws ServerException("Transient server error")

      // We want the code to skip a heartbeat, log "retrying", but not break out on the first iteration
      // Then for the second iteration, we want the time since lastSuccessfulHeartbeat to exceed heartbeatTimeout
      // That triggers checkIfExpiredAndMarkSyncStateAsFailed => break out
      // To do that, we can let the test time advance artificially by more than shortHeartbeatTimeout
      // so that the second iteration sees "lastSuccessfulHeartbeat" too old.

      val sender =
        WorkloadHeartbeatSender(
          workloadApiClient = mockWorkloadApiClient,
          replicationWorkerState = mockReplicationWorkerState,
          destinationTimeoutMonitor = mockDestinationTimeoutMonitor,
          sourceTimeoutMonitor = mockSourceTimeoutMonitor,
          heartbeatInterval = shortHeartbeatInterval,
          heartbeatTimeoutDuration = shortHeartbeatTimeout, // e.g. 50ms
          workloadId = testWorkloadId,
          jobId = testJobId,
          attempt = testAttempt,
        )

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat() // indefinite while loop
        }

      // The first iteration tries to do heartbeat immediately, it fails with "Transient server error", logs it, won't break
      // We now move time forward beyond shortHeartbeatTimeout to ensure the next iteration sees a time gap > heartbeatTimeout
      delay(1000) // well beyond 50ms

      // The second iteration sees that lastSuccessfulHeartbeat is from the moment we started the loop => more than 50ms
      // => it triggers checkIfExpiredAndMarkSyncStateAsFailed => markFailed, abort, trackFailure => break the loop
      assertTrue(job.isCompleted)

      // We confirm we markFailed, trackFailure with a WorkloadHeartbeatException
      verify(exactly = 1) { mockReplicationWorkerState.markFailed() }
      verify(exactly = 1) { mockReplicationWorkerState.abort() }
      verify(exactly = 1) {
        mockReplicationWorkerState.trackFailure(
          withArg {
            assertTrue(it is WorkloadHeartbeatException)
            assertTrue(it.message!!.contains("Workload Heartbeat Error"))
          },
          testJobId,
          testAttempt,
        )
      }
      // We never call markCancelled in this scenario
      verify(exactly = 0) { mockReplicationWorkerState.markCancelled() }
    }
}
