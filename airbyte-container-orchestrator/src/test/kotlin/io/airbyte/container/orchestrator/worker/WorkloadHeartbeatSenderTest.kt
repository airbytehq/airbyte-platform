/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.api.client.ApiException
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.micronaut.runtime.AirbyteContextConfig
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
internal class WorkloadHeartbeatSenderTest {
  private lateinit var mockWorkloadApiClient: WorkloadApiClient
  private lateinit var mockReplicationWorkerState: ReplicationWorkerState
  private lateinit var mockDestinationTimeoutMonitor: DestinationTimeoutMonitor
  private lateinit var mockSourceTimeoutMonitor: HeartbeatMonitor
  private lateinit var mockReplicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader
  private lateinit var hardExitCallable: () -> Unit
  private lateinit var source: AirbyteSource
  private lateinit var destination: AirbyteDestination

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
    mockReplicationInputFeatureFlagReader = mockk(relaxed = true)
    hardExitCallable = mockk(relaxed = true)
    source = mockk(relaxed = true)
    destination = mockk(relaxed = true)

    every { mockDestinationTimeoutMonitor.hasTimedOut() } returns false
    every { mockSourceTimeoutMonitor.hasTimedOut() } returns false
    every { mockReplicationInputFeatureFlagReader.read<Boolean>(any()) } returns false
  }

  @AfterEach
  fun teardown() {
    clearAllMocks()
  }

  /**
   * Demonstrates a normal single iteration: no timeouts, no errors, no abort.
   * We force the loop to break by letting the second iteration trigger a "workload in terminal state" (HTTP 410).
   * This ensures we only see one successful workloadRunning call and one successful heartbeat call first.
   */
  @Test
  fun `test normal flow - single successful heartbeat then GONE`() =
    runTest {
      // First call transitions to running, subsequent calls are heartbeats
      every { mockWorkloadApiClient.workloadRunning(any()) } returns Unit
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit andThen
        { throw ApiException(HttpStatus.GONE.code, "http://localhost.test", "") }

      val sender = getSender()

      // Launch the heartbeat in a child job so we can let it run, then see how it breaks on 2nd iteration
      val job = backgroundScope.launch { sender.sendHeartbeat() }

      // Iteration 0 (0ms): workloadRunning() succeeds
      // Iteration 1 (10ms): workloadHeartbeat() succeeds
      // Iteration 2 (20ms): workloadHeartbeat() throws GONE → exit
      advanceTimeBy(30) // ms - enough for 3 iterations

      // The job should complete because we get a GONE on second heartbeat
      assertTrue(job.isCompleted)

      // First call => workloadRunning
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadRunning(match { it.workloadId == testWorkloadId })
      }
      // Heartbeat called once successfully, second call throws GONE
      verify(exactly = 2) {
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
      // Running transition happens (hasTimedOut not checked during transition)
      // First iteration of main loop: timeout detected (hasTimedOut=true) → fail and break
      every { mockDestinationTimeoutMonitor.hasTimedOut() } returns true
      every { mockDestinationTimeoutMonitor.timeSinceLastAction } returns AtomicLong(0)

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadRunning(any()) } returns Unit
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for running transition + first loop iteration that detects timeout
      advanceTimeBy(20)

      // The job completes due to the destination heartbeat timeout
      assertTrue(job.isCompleted)

      // There should be one running call and no heartbeat before the destination timeout kicked in
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadRunning(any())
      }
      verify(exactly = 0) {
        mockWorkloadApiClient.workloadHeartbeat(any())
      }

      // We should have failed the workload because of the destination timeout failure
      verify(exactly = 1) { mockReplicationWorkerState.markFailed() }

      // Verify destination.close() was called to unblock any stuck reader threads
      verify(exactly = 1) { destination.close() }
    }

  /**
   * Similar scenario: skip sending heartbeat if source should fail on heartbeat failure.
   */
  @Test
  fun `fail the Workload if sourceTimeoutMonitor hasTimedOut`() =
    runTest {
      // Running transition happens (hasTimedOut not checked during transition)
      // First iteration of main loop: timeout detected (hasTimedOut=true) → fail and break
      every { mockSourceTimeoutMonitor.hasTimedOut() } returns true
      every { mockSourceTimeoutMonitor.timeSinceLastBeat } returns Optional.of(Duration.ZERO)

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadRunning(any()) } returns Unit
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for running transition + first loop iteration that detects timeout
      advanceTimeBy(20)

      // The job completes due to the source heartbeat timeout
      assertTrue(job.isCompleted)

      // There should be one running call and no heartbeat before the source timeout kicked in
      verify(exactly = 1) {
        mockWorkloadApiClient.workloadRunning(any())
      }
      verify(exactly = 0) {
        mockWorkloadApiClient.workloadHeartbeat(any())
      }

      // We should have failed the workload because of the source timeout failure
      verify(exactly = 1) { mockReplicationWorkerState.markFailed() }
    }

  @Test
  fun `do not fail the Workload if sourceTimeoutMonitor hasTimedOut, but the source finished first`() =
    runTest {
      // Running transition happens
      // First iteration of main loop: source timeout detected but source.isFinished=true → continue running, send heartbeat
      every { mockSourceTimeoutMonitor.hasTimedOut() } returns true
      every { mockSourceTimeoutMonitor.timeSinceLastBeat } returns Optional.of(Duration.ZERO)
      every { source.isFinished } returns true

      // We'll ensure that only a small time elapses so that we do NOT exceed the heartbeatTimeout
      // In this scenario, heartbeat as usual
      every { mockWorkloadApiClient.workloadRunning(any()) } returns Unit
      every { mockWorkloadApiClient.workloadHeartbeat(any()) } returns Unit

      val sender = getSender(heartbeatTimeoutDuration = Duration.ofMinutes(1))

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat()
        }

      // Let the test time move enough for running transition + first loop iteration that sends heartbeat
      advanceTimeBy(20)

      // The job is allowed to continue to run because source finished
      assertFalse(job.isCompleted)

      // We should NOT have failed the workload because source finished
      verify(exactly = 0) { mockReplicationWorkerState.markFailed() }
    }

  /**
   * If the server returns some other error (not GONE), we log and retry unless we exceed the total heartbeat timeout.
   * We'll force the iteration to exceed the heartbeat timeout. That triggers exit.
   */
  @Test
  fun `test general exception triggers retry, but eventually we exceed heartbeatTimeout and fail`() =
    runTest {
      // Let the workloadRunning call throw a generic error from the client repeatedly
      every { mockWorkloadApiClient.workloadRunning(any()) } throws
        ApiException(HttpStatus.INTERNAL_SERVER_ERROR.code, "http://localhost.test", "")

      // We want the code to retry, log "retrying", but not break out on the first iteration
      // Then after enough time, we want the time since lastSuccessfulHeartbeat to exceed heartbeatTimeout
      // That triggers exit

      val sender = getSender()

      val job =
        backgroundScope.launch {
          sender.sendHeartbeat() // infinite while loop trying to transition to running
        }

      // The first iteration tries to transition to running, it fails with "Transient server error", logs it, won't break
      // We now move time forward beyond shortHeartbeatTimeout to ensure the next iteration sees a time gap > heartbeatTimeout
      delay(1000) // well beyond 50ms

      // The iteration sees that lastSuccessfulHeartbeat is from the moment we started the loop => more than 50ms
      // => it triggers exit
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
      source = source,
      destination = destination,
      heartbeatInterval = shortHeartbeatInterval,
      heartbeatTimeoutDuration = heartbeatTimeoutDuration, // e.g. 50ms
      hardExitCallable = hardExitCallable,
      airbyteContextConfig = AirbyteContextConfig(attemptId = testAttempt, jobId = testJobId, workloadId = testWorkloadId),
      replicationInputFeatureFlagReader = mockReplicationInputFeatureFlagReader,
    )
}
