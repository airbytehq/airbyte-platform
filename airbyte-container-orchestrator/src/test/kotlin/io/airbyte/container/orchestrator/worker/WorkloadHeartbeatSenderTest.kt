/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.api.client.ApiException
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.workload.api.client.WorkloadApiClient
import io.micronaut.http.HttpStatus
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
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
  private lateinit var hardExitCallable: () -> Unit
  private lateinit var source: AirbyteSource

  private val testWorkloadId = "test-workload"
  private val testJobId = 0L
  private val testAttempt = 1
  private val shortHeartbeatInterval = Duration.ofMillis(10)
  private val shortHeartbeatTimeout = Duration.ofMillis(50)

  @BeforeEach
  fun setup() {
    mockWorkloadApiClient = mockk()
    mockReplicationWorkerState = mockk(relaxed = true)
    mockDestinationTimeoutMonitor = mockk(relaxed = true)
    mockSourceTimeoutMonitor = mockk(relaxed = true)
    hardExitCallable = mockk(relaxed = true)
    source = mockk(relaxed = true)

    every { mockDestinationTimeoutMonitor.hasTimedOut() } returns false
    every { mockSourceTimeoutMonitor.hasTimedOut() } returns false
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
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit
      // On the second call, it throws a ClientException with 410 => we markCancelled() and break
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } throws ApiException(HttpStatus.GONE.code, "http://localhost.test", "")

      val sender = getSender()

      // Launch the heartbeat in a child job so we can let it run, then see how it breaks on 2nd iteration
      val job = backgroundScope.launch { sender.sendHeartbeat() }

      // Advance enough time for the first heartbeat call, then the second iteration that triggers GONE
      advanceTimeBy(100) // ms

      // The job should complete because we get a GONE on second iteration
      assertTrue(job.isCompleted)

      // First call => success
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadHeartbeat(match { it.workloadId == testWorkloadId })
      }
      // Then we see a ClientException(410) => exit
      verify(exactly = 1) { hardExitCallable() }

      confirmVerified(mockReplicationWorkerState, mockWorkloadApiClient)
    }

  /**
   * We skip sending a heartbeat if the destination is timed out, but haven't yet exceeded the total heartbeat timeout
   * since lastSuccessfulHeartbeat. We'll let it do a second iteration that triggers a forced GONE to break the loop.
   */
  @Test
  fun `fail the Workload if destinationTimeoutMonitor hasTimedOut`() =
    runTest {
      // We want the first iteration to see "hasTimedOut = true" => skip heartbeat, but not break out yet
      every { mockDestinationTimeoutMonitor.hasTimedOut() } returnsMany listOf(false, true)
      every { mockDestinationTimeoutMonitor.timeSinceLastAction } returnsMany listOf(AtomicLong(0), AtomicLong(0))

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for 2 iterations
      advanceTimeBy(200)

      // The job completes due to the destination heartbeat timeout
      assertTrue(job.isCompleted)

      // There should be one successful heartbeat before the destination timeout kicked in
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadHeartbeat(any())
      }

      // We should have failed the workload because of the destination timeout failure
      verify(exactly = 1) { mockReplicationWorkerState.markFailed() }
    }

  /**
   * Similar scenario: skip sending heartbeat if source should fail on heartbeat failure.
   */
  @Test
  fun `fail the Workload if sourceTimeoutMonitor hasTimedOut`() =
    runTest {
      // We want the first iteration to see "hasTimedOut = true" => skip heartbeat, but not break out yet
      every { mockSourceTimeoutMonitor.hasTimedOut() } returnsMany listOf(false, true)
      every { mockSourceTimeoutMonitor.timeSinceLastBeat } returnsMany
        listOf(
          Optional.of(Duration.ZERO),
          Optional.of(Duration.ZERO),
        )

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for 2 iterations
      advanceTimeBy(200)

      // The job completes due to the destination heartbeat timeout
      assertTrue(job.isCompleted)

      // There should be one successful heartbeat before the source timeout kicked in
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadHeartbeat(any())
      }

      // We should have failed the workload because of the source timeout failure
      verify(exactly = 1) { mockReplicationWorkerState.markFailed() }
    }

  @Test
  fun `do not fail the Workload if sourceTimeoutMonitor hasTimedOut, but the source finished first`() =
    runTest {
      // We want the first iteration to see "hasTimedOut = true" => skip heartbeat, but not break out yet
      every { mockSourceTimeoutMonitor.hasTimedOut() } returnsMany listOf(false, true)
      every { mockSourceTimeoutMonitor.timeSinceLastBeat } returnsMany
        listOf(
          Optional.of(Duration.ZERO),
          Optional.of(Duration.ZERO),
        )
      every { source.isFinished } returns true

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for 2 iterations
      advanceTimeBy(200)

      // The job is allowed to continue to run
      assertFalse(job.isCompleted)

      // We should have failed the workload because of the source timeout failure
      verify(exactly = 0) { mockReplicationWorkerState.markFailed() }
    }

  /**
   * If the server returns some other error (not GONE), we log and retry unless we exceed the total heartbeat timeout.
   * We'll force the second iteration to exceed the heartbeat timeout. That triggers markFailed, abort, trackFailure.
   */
  @Test
  fun `test general exception triggers retry, but eventually we exceed heartbeatTimeout and fail`() =
    runTest {
      // Let the first iteration throw a generic error from the client.
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } throws
        ApiException(HttpStatus.INTERNAL_SERVER_ERROR.code, "http://localhost.test", "")

      // We want the code to skip a heartbeat, log "retrying", but not break out on the first iteration
      // Then for the second iteration, we want the time since lastSuccessfulHeartbeat to exceed heartbeatTimeout
      // That triggers checkIfExpiredAndMarkSyncStateAsFailed => break out
      // To do that, we can let the test time advance artificially by more than shortHeartbeatTimeout
      // so that the second iteration sees "lastSuccessfulHeartbeat" too old.

      val sender = getSender()

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat() // infinite while loop
        }

      // The first iteration tries to do heartbeat immediately, it fails with "Transient server error", logs it, won't break
      // We now move time forward beyond shortHeartbeatTimeout to ensure the next iteration sees a time gap > heartbeatTimeout
      delay(1000) // well beyond 50ms

      // The second iteration sees that lastSuccessfulHeartbeat is from the moment we started the loop => more than 50ms
      // => it triggers checkIfExpiredAndMarkSyncStateAsFailed => markFailed, abort, trackFailure => break the loop
      assertTrue(job.isCompleted)

      // We confirm we hardExit
      verify(exactly = 1) { hardExitCallable() }
    }

  private fun getSender(heartbeatTimeoutDuration: Duration = shortHeartbeatTimeout) =
    WorkloadHeartbeatSender(
      workloadApiClient = mockWorkloadApiClient,
      replicationWorkerState = mockReplicationWorkerState,
      destinationTimeoutMonitor = mockDestinationTimeoutMonitor,
      sourceTimeoutMonitor = mockSourceTimeoutMonitor,
      heartbeatInterval = shortHeartbeatInterval,
      heartbeatTimeoutDuration = heartbeatTimeoutDuration, // e.g. 50ms
      workloadId = testWorkloadId,
      jobId = testJobId,
      attempt = testAttempt,
      hardExitCallable = hardExitCallable,
      source = source,
    )
}
