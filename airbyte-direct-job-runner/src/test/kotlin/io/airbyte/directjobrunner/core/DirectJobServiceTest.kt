/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
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
    }
  private val workloadId = "workload-1"
  private val payload = """{"host":"db.example.com"}"""

  private lateinit var executor: ThreadPoolExecutor
  private lateinit var service: DirectJobService

  @BeforeEach
  fun setup() {
    // Mirrors a Micronaut `fixed` executor: a fixed number of threads with an unbounded queue.
    executor = ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())
    service = DirectJobService(workloadApiClient, executor, listOf(handler))
  }

  @AfterEach
  fun teardown() {
    executor.shutdownNow()
  }

  private fun workload(
    status: WorkloadStatus?,
    id: String = workloadId,
    type: WorkloadType = WorkloadType.CONNECT,
  ) = Workload(id = id, status = status, type = type, inputPayload = payload)

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
        handler.run(id, payload)
        workloadApiClient.workloadSuccess(WorkloadSuccessRequest(id))
      }
    }
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
    verify(exactly = 0) { handler.run(any(), any()) }
  }

  @Test
  fun `a workload type without a handler is unsupported and nothing is run`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED, type = WorkloadType.SYNC)

    assertEquals(DirectJobSubmission.Unsupported(WorkloadType.SYNC), service.submit(workloadId))

    verify(exactly = 0) { workloadApiClient.workloadRunning(any()) }
    verify(exactly = 0) { handler.run(any(), any()) }
    // The workload is released, so a later submission is evaluated again rather than reported as a duplicate.
    assertEquals(DirectJobSubmission.Unsupported(WorkloadType.SYNC), service.submit(workloadId))
  }

  @Test
  fun `two handlers for the same type are rejected at startup`() {
    val other =
      mockk<DirectJobHandler> {
        every { workloadType } returns WorkloadType.CONNECT
      }

    assertThrows<IllegalArgumentException> { DirectJobService(workloadApiClient, executor, listOf(handler, other)) }
  }

  @Test
  fun `a workload that is already in flight is a duplicate`() {
    val release = CountDownLatch(1)
    val started = CountDownLatch(1)
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload) } answers {
      started.countDown()
      release.await(5, TimeUnit.SECONDS)
    }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    assertTrue(started.await(5, TimeUnit.SECONDS))
    assertEquals(DirectJobSubmission.Duplicate, service.submit(workloadId))
    release.countDown()
    awaitIdle()

    verify(exactly = 1) { handler.run(any(), any()) }
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
    every { handler.run("a", payload) } answers {
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
    every { handler.run(workloadId, payload) } answers { release.await(5, TimeUnit.SECONDS) }

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
    verify(exactly = 0) { handler.run(any(), any()) }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `a handler failure fails the workload as a platform error`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload) } throws IllegalStateException("boom")

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
  }

  @Test
  fun `a failure while failing the workload does not leak the in-flight entry`() {
    every { workloadApiClient.workloadGet(workloadId) } returns workload(WorkloadStatus.CLAIMED)
    every { handler.run(workloadId, payload) } throws IllegalStateException("boom")
    every { workloadApiClient.workloadFailure(any()) } throws RuntimeException("api down")

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
    awaitCompletedTasks(1)

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }

  @Test
  fun `workload api errors on get propagate and release the workload`() {
    every { workloadApiClient.workloadGet(workloadId) } throws RuntimeException("api down") andThen workload(WorkloadStatus.CLAIMED)

    assertThrows<RuntimeException> { service.submit(workloadId) }

    assertEquals(DirectJobSubmission.Accepted, service.submit(workloadId))
  }
}
