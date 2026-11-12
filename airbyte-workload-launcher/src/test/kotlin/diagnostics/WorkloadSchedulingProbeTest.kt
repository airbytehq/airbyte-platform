/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.fabric8.kubernetes.client.KubernetesClientException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val MEMORY_MESSAGE = "0/3 nodes are available: 3 Insufficient memory."

internal class WorkloadSchedulingProbeTest {
  private val dataplane =
    DataplaneConfig(
      dataplaneId = UUID.randomUUID(),
      dataplaneName = "dataplane",
      dataplaneEnabled = true,
      dataplaneGroupId = UUID.randomUUID(),
      dataplaneGroupName = "region",
      organizationId = UUID.randomUUID(),
    )
  private val start: Instant = Instant.parse("2026-11-10T12:00:00Z")
  private var now: Instant = start
  private val clock = mockk<Clock> { every { instant() } answers { now } }
  private val workloadPods = mockk<WorkloadPodLister>()

  private val blockedSince = start.minus(Duration.ofMinutes(5))
  private val scheduledAt = start.minus(Duration.ofMinutes(10))
  private val blockedPods =
    listOf(
      workloadPod("blocked", condition = unschedulable(blockedSince)),
      workloadPod("done", jobType = "check", phase = "Succeeded", condition = scheduled(scheduledAt)),
    )

  @Test
  fun `an empty namespace is a passed scan with nothing observed yet`() {
    every { workloadPods.list() } returns emptyList()

    val probe = probe()
    probe.scan()

    assertEquals(listOf(result(DataplaneDiagnosticStatus.PASSED, start, mapOf("blocked_count" to 0))), probe.latestCompletedResults())
  }

  @Test
  fun `blocked pods fail the scan with a customer-safe error and the observation`() {
    every { workloadPods.list() } returns blockedPods

    val probe = probe()
    probe.scan()

    assertEquals(
      listOf(
        result(
          DataplaneDiagnosticStatus.FAILED,
          start,
          blockedObservation(),
          errorCode = WorkloadSchedulingProbe.UNSCHEDULABLE,
          errorMessage = "1 workload pod has waited more than 2 minutes for a node: $MEMORY_MESSAGE",
        ),
      ),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `a blocked pod without a scheduler message fails the scan without a reason`() {
    every { workloadPods.list() } returns listOf(workloadPod("blocked", condition = unschedulable(blockedSince, message = "")))

    val probe = probe()
    probe.scan()

    val results = probe.latestCompletedResults()
    assertEquals(
      listOf("1 workload pod has waited more than 2 minutes for a node."),
      results.map { it.errorMessage },
      "$results",
    )
  }

  @Test
  fun `a clean scan after a problem passes without errors and keeps the problem and the last scheduling`() {
    every { workloadPods.list() } returns blockedPods
    val probe = probe()
    probe.scan()

    now = start.plusSeconds(60)
    every { workloadPods.list() } returns emptyList()
    probe.scan()

    assertEquals(
      listOf(
        result(
          DataplaneDiagnosticStatus.PASSED,
          now,
          mapOf("blocked_count" to 0, "last_scheduled" to lastScheduled(), "last_problem" to lastProblem(observedAt = start)),
        ),
      ),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `a scan that can't list pods leaves out the counts, keeps earlier events and reports a safe error`() {
    every { workloadPods.list() } returns blockedPods
    val probe = probe()
    probe.scan()

    now = start.plusSeconds(60)
    every { workloadPods.list() } throws KubernetesClientException("connect to 10.0.0.1:443 failed: token abc123", 500, null)
    probe.scan()

    assertEquals(
      listOf(
        result(
          DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
          now,
          mapOf("last_scheduled" to lastScheduled(), "last_problem" to lastProblem(observedAt = start)),
          errorCode = WorkloadSchedulingProbe.POD_LIST_FAILED,
          errorMessage = "The launcher could not list pods in its jobs namespace.",
        ),
      ),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `a first scan that is forbidden to list pods reports access denied and no count`() {
    every { workloadPods.list() } throws KubernetesClientException("pods is forbidden", 403, null)

    val probe = probe()
    probe.scan()

    assertEquals(
      listOf(
        result(
          DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
          start,
          emptyMap(),
          errorCode = WorkloadSchedulingProbe.ACCESS_DENIED,
          errorMessage = "The launcher is not allowed to list pods in its jobs namespace.",
        ),
      ),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `scans that can't complete never expire the problem, and the next clean scan does`() {
    every { workloadPods.list() } returns blockedPods
    val probe = probe()
    probe.scan()
    now = start.plusSeconds(60)
    every { workloadPods.list() } returns emptyList()
    probe.scan()

    now = start.plus(Duration.ofHours(2))
    every { workloadPods.list() } throws KubernetesClientException("timeout", 0, null)
    probe.scan()
    assertEquals(
      lastProblem(observedAt = start),
      probe
        .latestCompletedResults()
        .single()
        .observation
        ?.get("last_problem"),
    )

    now = start.plus(Duration.ofHours(2)).plusSeconds(60)
    every { workloadPods.list() } returns emptyList()
    probe.scan()
    assertEquals(
      listOf(result(DataplaneDiagnosticStatus.PASSED, now, mapOf("blocked_count" to 0, "last_scheduled" to lastScheduled()))),
      probe.latestCompletedResults(),
    )
  }

  @Test
  fun `heartbeats read the same completed result until the next scan finishes`() {
    every { workloadPods.list() } returns emptyList()
    val probe = probe()
    probe.scan()
    val completed = probe.latestCompletedResults()

    val listing = CountDownLatch(1)
    val release = CountDownLatch(1)
    every { workloadPods.list() } answers {
      listing.countDown()
      release.await()
      blockedPods
    }
    now = start.plusSeconds(60)
    val scan = CompletableFuture.runAsync { probe.scan() }
    assertTrue(listing.await(5, TimeUnit.SECONDS), "the scan never started listing pods")

    assertEquals(completed, probe.latestCompletedResults())
    assertEquals(completed, probe.latestCompletedResults())

    release.countDown()
    scan.get(5, TimeUnit.SECONDS)
    assertEquals(DataplaneDiagnosticStatus.FAILED, probe.latestCompletedResults().single().status)
  }

  @Test
  fun `does nothing before the data plane is known`() {
    val probe = WorkloadSchedulingProbe(workloadPods, clock)

    probe.scan()

    assertEquals(emptyList<DataplaneDiagnosticResult>(), probe.latestCompletedResults())
    verify(exactly = 0) { workloadPods.list() }
  }

  @Test
  fun `scans a data plane that has no organization`() {
    every { workloadPods.list() } returns emptyList()
    val probe = WorkloadSchedulingProbe(workloadPods, clock)
    probe.onApplicationEvent(dataplane.copy(organizationId = null))

    probe.scan()

    assertEquals(listOf(result(DataplaneDiagnosticStatus.PASSED, start, mapOf("blocked_count" to 0))), probe.latestCompletedResults())
  }

  private fun probe() = WorkloadSchedulingProbe(workloadPods, clock).also { it.onApplicationEvent(dataplane) }

  private fun blockedObservation() =
    mapOf(
      "blocked_count" to 1,
      "longest_blocked_since" to blockedSince.toEpochMilli(),
      "longest_blocked_workload_type" to "SYNC",
      "last_scheduled" to lastScheduled(),
      "last_problem" to lastProblem(observedAt = start),
    )

  private fun lastScheduled() = mapOf("scheduled_at" to scheduledAt.toEpochMilli(), "workload_type" to "CHECK")

  private fun lastProblem(observedAt: Instant) =
    mapOf("observed_at" to observedAt.toEpochMilli(), "reason" to "Unschedulable", "message" to MEMORY_MESSAGE)

  private fun result(
    status: DataplaneDiagnosticStatus,
    checkedAt: Instant,
    observation: Map<String, Any>,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.WORKLOAD_SCHEDULING.value,
    targetId = dataplane.dataplaneId,
    status = status,
    checkedAt = checkedAt.toEpochMilli(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = observation,
  )
}
