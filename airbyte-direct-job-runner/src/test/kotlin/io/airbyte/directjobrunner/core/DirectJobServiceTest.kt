/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class DirectJobServiceTest {
  private val workloadApiClient = mockk<WorkloadApiClient>(relaxed = true)
  private val handler =
    mockk<DirectJobHandler>(relaxed = true) {
      every { workloadType } returns WorkloadType.CONNECT
      every { runningDeadline } returns Duration.ofMinutes(2)
      every { accepts(any()) } returns true
    }
  private val workloadId = "workload-1"
  private val payload = """{"host":"db.example.com"}"""
  private val logPath = "/workspace/workload-1/logs.log"
  private val meterRegistry = SimpleMeterRegistry()
  private val metricClient = MetricClient(meterRegistry)

  private lateinit var executor: ThreadPoolExecutor
  private lateinit var checkExecutor: ThreadPoolExecutor
  private lateinit var service: DirectJobService

  @BeforeEach
  fun setup() {
    // Mirrors a Micronaut `fixed` executor: a fixed number of threads with an unbounded queue.
    executor = ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())
    checkExecutor = ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())
    service = DirectJobService(workloadApiClient, executor, checkExecutor, listOf(handler), metricClient)
  }

  @AfterEach
  fun teardown() {
    executor.shutdownNow()
    checkExecutor.shutdownNow()
  }

  private fun workload(
    status: WorkloadStatus?,
    id: String = workloadId,
    type: WorkloadType = WorkloadType.CONNECT,
  ) = Workload(id = id, status = status, type = type, inputPayload = payload, logPath = logPath)

  private fun submissions(
    result: String,
    workloadType: String = "connect",
  ): Double =
    meterRegistry
      .find("direct_job_submission")
      .tags("workload_type", workloadType, "result", result)
      .counter()
      ?.count() ?: 0.0

  private fun runs(result: String): Double =
    meterRegistry
      .find("direct_job_run")
      .tags("workload_type", "connect", "result", result)
      .counter()
      ?.count() ?: 0.0

  private fun failureReportErrors(): Double =
    meterRegistry
      .find("direct_job_failure_report_error")
      .tags("workload_type", "connect")
      .counter()
      ?.count() ?: 0.0

  private fun inFlightGauge(): Double = meterRegistry.find("direct_job_in_flight").gauge()!!.value()

  private fun awaitIdle() {
    executor.shutdown()
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
  }

  // activeCount can still read 0 before a just-submitted task starts, so wait on completed tasks instead.
  private fun awaitCompletedTasks(count: Long) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (executor.completedTaskCount < count) {
      check(System.nanoTime() < deadline) { "timed out waiting for $count completed tasks" }
      Thread.sleep(5)
    }
  }

  @Test
  fun `claimed and launched workloads are accepted and run to success`() {
    listOf(WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED).forEachIndexed { index, status ->
      val id = "workload-$index"
      every { workloadApiClient.workloadGet(id) } returns workload(status, id)

      assertEquals(DirectJobSubmission.Accepted, service.submit(id))
      awaitCompletedTasks(index + 1L)

      verifyOrder {
        workloadApiClient.workloadRunning(match<WorkloadRunningRequest> { it.workloadId == id && it.deadline != null })
        handler.run(id, payload, logPath)
        workloadApiClient.workloadSuccess(WorkloadSuccessRequest(id))
      }
    }

    assertEquals(2.0, submissions("accepted"))
    assertEquals(2.0, runs("success"))
    assertEquals(
      2L,
      meterRegistry
        .find("direct_job_run_duration")
        .tags("result", "success")
        .summary()!!
        .count(),
    )
    assertEquals(
      2L,
      meterRegistry
        .find("direct_job_queue_wait")
        .tags("workload_type", "connect")
        .summary()!!
        .count(),
    )
    assertEquals(0.0, inFlightGauge())
  }

  @Test
  fun `the running deadline comes from the handler`() {
    every { handler.runningDeadline } returns Duration.ofMinutes(30)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)

    val before = OffsetDateTime.now()
    service.submit(workloadId)

    verify {
      workloadApiClient.workloadRunning(
        match<WorkloadRunningRequest> { it.deadline!!.isAfter(before.plusMinutes(29)) },
      )
    }
  }

  @Test
  fun `other statuses are not eligible and nothing is run`() {
    listOf(
      WorkloadStatus.PENDING,
      WorkloadStatus.RUNNING,
      WorkloadStatus.SUCCESS,
      WorkloadStatus.FAILURE,
      WorkloadStatus.CANCELLED,
      null,
    ).forEach { status ->
      every { workloadApiClient.workloadGet(workloadId) } returns workload(status)

      assertEquals(DirectJobSubmission.NotEligible(status), service.submit(workloadId))
    }

    verify(exactly = 0) { workloadApiClient.workloadRunning(any()) }
    verify(exactly = 0) { handler.run(any(), any(), any()) }
    assertEquals(6.0, submissions("not_eligible"))
  }

  @Test
  fun `a workload type without a handler is unsupported and nothing is run`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED, type = WorkloadType.SYNC)

    assertEquals(DirectJobSubmission.Unsupported(WorkloadType.SYNC), service.submit(workloadId))

    verify(exactly = 0) { workloadApiClient.workloadRunning(any()) }
    verify(exactly = 0) { handler.run(any(), any(), any()) }
    // The workload is released, so a later submission is evaluated again rather than reported as a duplicate.
    assertEquals(DirectJobSubmission.Unsupported(WorkloadType.SYNC), service.submit(workloadId))
    assertEquals(2.0, submissions("unsupported", workloadType = "sync"))
  }

  @Test
  fun `two handlers for the same type are rejected at startup`() {
    val other =
      mockk<DirectJobHandler> {
        every { workloadType } returns WorkloadType.CONNECT
      }

    assertThrows<IllegalArgumentException> { DirectJobService(workloadApiClient, executor, checkExecutor, listOf(handler, other), metricClient) }
  }

  @Test
  fun `a workload that is already in flight is a duplicate`() {
    val release = CountDownLatch(1)
    val started = CountDownLatch(1)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload, logPath) } answers {
      started.countDown()
      release.await(5, TimeUnit.SECONDS)
    }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    assertTrue(started.await(5, TimeUnit.SECONDS))
    assertEquals(DirectJobSubmission.Duplicate, service.submit(workloadId))
    // The duplicate is rejected before the workload is read, so its type is unknown.
    assertEquals(1.0, submissions("duplicate", workloadType = "unknown"))
    assertEquals(1.0, inFlightGauge())
    release.countDown()
    awaitIdle()

    verify(exactly = 1) { handler.run(any(), any(), any()) }
    assertEquals(0.0, inFlightGauge())
  }

  @Test
  fun `workload can be accepted again after it finished`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitCompletedTasks(1)

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `work beyond the pool size queues and still runs`() {
    val release = CountDownLatch(1)
    val started = CountDownLatch(1)
    every { workloadApiClient.workloadGet("a") } returns workload(WorkloadStatus.CLAIMED, "a")
    every { workloadApiClient.workloadGet("b") } returns workload(WorkloadStatus.CLAIMED, "b")
    every { handler.run("a", payload, logPath) } answers {
      started.countDown()
      release.await(5, TimeUnit.SECONDS)
    }

    assertEquals(DirectJobSubmission.Accepted, service.submit("a"))
    assertTrue(started.await(5, TimeUnit.SECONDS))
    assertEquals(DirectJobSubmission.Accepted, service.submit("b"))
    release.countDown()
    awaitCompletedTasks(2)

    verify(exactly = 1) { workloadApiClient.workloadSuccess(WorkloadSuccessRequest("a")) }
    verify(exactly = 1) { workloadApiClient.workloadSuccess(WorkloadSuccessRequest("b")) }
  }

  @Test
  fun `the workload is marked running before submit returns`() {
    val release = CountDownLatch(1)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload, logPath) } answers { release.await(5, TimeUnit.SECONDS) }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))

    // The job is still blocked, so this call can only have come from submit itself.
    verify(exactly = 1) { workloadApiClient.workloadRunning(match<WorkloadRunningRequest> { it.workloadId == workloadId }) }
    verify(exactly = 0) { workloadApiClient.workloadSuccess(any()) }
    release.countDown()
    awaitCompletedTasks(1)
  }

  @Test
  fun `a failed running update propagates, runs nothing, and releases the workload`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { workloadApiClient.workloadRunning(any()) } throws RuntimeException("gone") andThen Unit

    assertThrows<RuntimeException> { service.submit(workloadId) }
    verify(exactly = 0) { handler.run(any(), any(), any()) }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `a handler failure fails the workload as a platform error`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload, logPath) } throws IllegalStateException("boom")

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitIdle()

    verify(exactly = 1) {
      workloadApiClient.workloadFailure(
        match<WorkloadFailureRequest> {
          it.workloadId == workloadId && it.source == "airbyte_platform" && it.reason?.contains("boom") == true
        },
      )
    }
    verify(exactly = 0) { workloadApiClient.workloadSuccess(any()) }
    assertEquals(1.0, runs("failure"))
    assertEquals(0.0, runs("success"))
    assertEquals(0.0, failureReportErrors())
    assertEquals(1L, runDurations("failure")!!.count())
  }

  private fun runDurations(result: String) =
    meterRegistry
      .find("direct_job_run_duration")
      .tags("workload_type", "connect", "result", result)
      .summary()

  @Test
  fun `the run duration excludes reporting the result`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { workloadApiClient.workloadSuccess(any()) } answers { Thread.sleep(300) }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitIdle()

    val durations = runDurations("success")!!
    assertEquals(1L, durations.count())
    assertTrue(durations.max() < 300.0, "run duration ${durations.max()}ms includes the success report")
  }

  @Test
  fun `a failed success report counts as a failed run`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { workloadApiClient.workloadSuccess(any()) } throws RuntimeException("api down")

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitIdle()

    verify(exactly = 1) { workloadApiClient.workloadFailure(any()) }
    assertEquals(1.0, runs("failure"))
    // The handler itself succeeded, so its duration is tagged with its own result.
    assertEquals(1L, runDurations("success")!!.count())
    assertNull(runDurations("failure"))
  }

  @Test
  fun `a failure while failing the workload does not leak the in-flight entry`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload, logPath) } throws IllegalStateException("boom")
    every { workloadApiClient.workloadFailure(any()) } throws RuntimeException("api down")

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitCompletedTasks(1)

    assertEquals(1.0, failureReportErrors())
    assertEquals(1.0, runs("failure"))
    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `workload api errors on get propagate and release the workload`() {
    every { workloadApiClient.workloadGet(workloadId) } throws RuntimeException("api down") andThen workload(WorkloadStatus.CLAIMED)

    assertThrows<RuntimeException> { service.submit(workloadId) }
    // A submission that throws reaches the launcher as a 5xx and records no submission result.
    assertNull(meterRegistry.find("direct_job_submission").counter())

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `the handler gets the workload's log path`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)

    service.submit(workloadId)
    awaitIdle()

    verify(exactly = 1) { handler.run(workloadId, payload, "/workspace/workload-1/logs.log") }
  }

  @Test
  fun `a workload the handler can't run is unsupported and is not marked running`() {
    every { handler.accepts(any()) } returns false
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)

    assertEquals(DirectJobSubmission.Unsupported(WorkloadType.CONNECT), service.submit(workloadId))

    verify(exactly = 0) {
      workloadApiClient.workloadRunning(any())
      handler.run(any(), any(), any())
    }
    // The workload is released, so a later submission is judged again rather than reported as a duplicate.
    every { handler.accepts(any()) } returns true
    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `drain stops new submissions and waits for running jobs`() {
    val release = CountDownLatch(1)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { workloadApiClient.workloadGet("late") } returns workload(WorkloadStatus.CLAIMED, "late")
    every { handler.run(workloadId, payload, logPath) } answers { release.await(5, TimeUnit.SECONDS) }
    service.submit(workloadId)

    val drained = Thread { service.drain() }.apply { start() }
    // drain sets the flag before it waits, so give it a moment to start.
    Thread.sleep(100)
    assertEquals(DirectJobSubmission.ShuttingDown, service.submit("late"))
    assertTrue(drained.isAlive, "drain returned while a job was still running")

    release.countDown()
    drained.join(5_000)
    assertFalse(drained.isAlive, "drain did not return after the job finished")
    verify(exactly = 1) { workloadApiClient.workloadSuccess(WorkloadSuccessRequest(workloadId)) }
    verify(exactly = 0) { workloadApiClient.workloadGet("late") }
  }

  @Test
  fun `drain gives up after the timeout`() {
    val release = CountDownLatch(1)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload, logPath) } answers { release.await(5, TimeUnit.SECONDS) }
    service = DirectJobService(workloadApiClient, executor, checkExecutor, listOf(handler), metricClient, Duration.ofMillis(300))
    service.submit(workloadId)

    val start = System.nanoTime()
    service.drain()

    assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(2))
    release.countDown()
  }

  @Test
  fun `checks run on their own pool, so they never hold up other jobs`() {
    val checkHandler =
      mockk<DirectJobHandler>(relaxed = true) {
        every { workloadType } returns WorkloadType.CHECK
        every { runningDeadline } returns Duration.ofMinutes(2)
        every { accepts(any()) } returns true
      }
    val release = CountDownLatch(1)
    every { checkHandler.run("check", payload, logPath) } answers { release.await(5, TimeUnit.SECONDS) }
    every { workloadApiClient.workloadGet("check") } returns workload(WorkloadStatus.CLAIMED, "check", WorkloadType.CHECK)
    every { workloadApiClient.workloadGet("connect") } returns workload(WorkloadStatus.CLAIMED, "connect")
    service = DirectJobService(workloadApiClient, executor, checkExecutor, listOf(handler, checkHandler), metricClient)

    // The check holds the only check thread; the connect job still runs on the other pool.
    service.submit("check")
    service.submit("connect")
    awaitCompletedTasks(1)

    assertEquals(0L, checkExecutor.completedTaskCount)
    verify(exactly = 1) { handler.run("connect", payload, logPath) }
    release.countDown()
  }

  @Test
  fun `the deadline starts over when a queued job starts`() {
    every { handler.runningDeadline } returns Duration.ofSeconds(90)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)

    service.submit(workloadId)
    val startedAt = OffsetDateTime.now()
    awaitIdle()

    verifyOrder {
      workloadApiClient.workloadRunning(any())
      workloadApiClient.workloadHeartbeat(match { it.workloadId == workloadId && it.deadline!! >= startedAt.plusSeconds(89) })
      handler.run(workloadId, payload, logPath)
    }
  }

  @Test
  fun `a job that expired or was cancelled while queued is not run`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED) andThen workload(WorkloadStatus.FAILURE)
    every { workloadApiClient.workloadHeartbeat(any()) } throws RuntimeException("not running")

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitIdle()

    verify(exactly = 0) {
      handler.run(any(), any(), any())
      workloadApiClient.workloadSuccess(any())
      workloadApiClient.workloadFailure(any())
    }
  }

  @Test
  fun `a failed heartbeat on a job that is still running does not stop it`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED) andThen workload(WorkloadStatus.RUNNING)
    every { workloadApiClient.workloadHeartbeat(any()) } throws RuntimeException("api blip")

    service.submit(workloadId)
    awaitIdle()

    verify(exactly = 1) { handler.run(workloadId, payload, logPath) }
  }
}
