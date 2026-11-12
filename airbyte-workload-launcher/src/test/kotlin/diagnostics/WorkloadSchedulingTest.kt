/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodCondition
import io.fabric8.kubernetes.api.model.PodConditionBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.time.Duration
import java.time.Instant

private val NOW: Instant = Instant.parse("2026-11-10T12:00:00Z")
private const val MEMORY_MESSAGE = "0/3 nodes are available: 3 Insufficient memory."

internal class WorkloadSchedulingTest {
  @Test
  fun `a pod unschedulable for exactly the grace period is not blocked yet`() {
    val atGrace = workloadPod("at-grace", condition = unschedulable(NOW.minus(SCHEDULING_GRACE_PERIOD)))
    val pastGrace = workloadPod("past-grace", condition = unschedulable(NOW.minus(SCHEDULING_GRACE_PERIOD).minusMillis(1)))

    assertEquals(
      listOf(blocked("past-grace", NOW.minus(SCHEDULING_GRACE_PERIOD).minusMillis(1))),
      scanPods(listOf(atGrace, pastGrace), NOW).blocked,
    )
  }

  @Test
  fun `the wait is measured from pod creation when the condition has no transition time`() {
    val createdPastGrace = NOW.minus(SCHEDULING_GRACE_PERIOD).minusSeconds(1)
    val pastGrace = workloadPod("past-grace", createdAt = createdPastGrace, condition = unschedulable(since = null))
    val atGrace = workloadPod("at-grace", createdAt = NOW.minus(SCHEDULING_GRACE_PERIOD), condition = unschedulable(since = null))

    assertEquals(listOf(blocked("past-grace", createdPastGrace)), scanPods(listOf(pastGrace, atGrace), NOW).blocked)
  }

  @Test
  fun `finished and deleting pods are never blocked`() {
    val longAgo = NOW.minus(Duration.ofHours(1))
    val pods =
      listOf(
        workloadPod("succeeded", phase = "Succeeded", condition = unschedulable(longAgo)),
        workloadPod("failed", phase = "Failed", condition = unschedulable(longAgo)),
        workloadPod("deleting", deletedAt = NOW, condition = unschedulable(longAgo)),
      )

    assertEquals(emptyList<BlockedPod>(), scanPods(pods, NOW).blocked)
  }

  @Test
  fun `a pod waiting for another reason than Unschedulable is not blocked`() {
    val gated = workloadPod("gated", condition = unschedulable(NOW.minus(Duration.ofHours(1))).toBuilder().withReason("SchedulingGated").build())

    assertEquals(emptyList<BlockedPod>(), scanPods(listOf(gated), NOW).blocked)
  }

  @Test
  fun `counts each blocked pod among mixed pods and reports the one that has waited longest`() {
    val oldest = NOW.minus(Duration.ofMinutes(5))
    val pods =
      listOf(
        workloadPod("blocked-5m", jobType = "discover", condition = unschedulable(oldest)),
        workloadPod("blocked-3m", condition = unschedulable(NOW.minus(Duration.ofMinutes(3)))),
        workloadPod("within-grace", condition = unschedulable(NOW.minusSeconds(30))),
        workloadPod("running", phase = "Running", condition = scheduled(NOW.minus(Duration.ofMinutes(1)))),
        workloadPod("no-condition"),
      )

    val state = WorkloadSchedulingState().after(scanPods(pods, NOW), NOW)

    assertEquals(listOf("blocked-5m", "blocked-3m"), state.blockedPods.map { it.name })
    assertEquals(blocked("blocked-5m", oldest, workloadType = "DISCOVER"), state.longestBlocked)
  }

  @Test
  fun `pods blocked since the same moment are ordered by name to pick the longest wait`() {
    val since = NOW.minus(Duration.ofMinutes(5))
    val pods =
      listOf(workloadPod("b-sync", condition = unschedulable(since)), workloadPod("a-check", jobType = "check", condition = unschedulable(since)))

    val state = WorkloadSchedulingState().after(scanPods(pods, NOW), NOW)

    assertEquals(blocked("a-check", since, workloadType = "CHECK"), state.longestBlocked)
  }

  @Test
  fun `nothing blocked has no longest wait`() {
    val state = WorkloadSchedulingState().after(scanPods(listOf(workloadPod("running", phase = "Running", condition = scheduled(NOW))), NOW), NOW)

    assertNull(state.longestBlocked)
  }

  @Test
  fun `the last scheduling is the newest one across pods, including finished ones`() {
    val pods =
      listOf(
        workloadPod("sync", jobType = "sync", phase = "Running", condition = scheduled(NOW.minus(Duration.ofMinutes(5)))),
        workloadPod("discover", jobType = "discover", phase = "Succeeded", condition = scheduled(NOW.minus(Duration.ofMinutes(1)))),
        workloadPod("no-time", jobType = "check", phase = "Running", condition = scheduled(at = null)),
      )

    assertEquals(ScheduledEvent(NOW.minus(Duration.ofMinutes(1)), "DISCOVER"), scanPods(pods, NOW).lastScheduled)
  }

  @Test
  fun `the last scheduling is kept through idle scans and replaced only by a newer one`() {
    val first = NOW.minus(Duration.ofMinutes(10))
    var state = WorkloadSchedulingState().after(scanPods(listOf(workloadPod("a", jobType = "check", condition = scheduled(first))), NOW), NOW)

    state = state.after(scanPods(emptyList(), NOW.plusSeconds(60)), NOW.plusSeconds(60))
    assertEquals(ScheduledEvent(first, "CHECK"), state.lastScheduled)

    state =
      state.after(scanPods(listOf(workloadPod("older", condition = scheduled(first.minusSeconds(1)))), NOW.plusSeconds(120)), NOW.plusSeconds(120))
    assertEquals(ScheduledEvent(first, "CHECK"), state.lastScheduled)

    val newer = NOW.plusSeconds(150)
    state = state.after(scanPods(listOf(workloadPod("newer", condition = scheduled(newer))), NOW.plusSeconds(180)), NOW.plusSeconds(180))
    assertEquals(ScheduledEvent(newer, "SYNC"), state.lastScheduled)
  }

  @Test
  fun `a problem keeps the time of the scan that first found it while the pod stays blocked`() {
    val podA = workloadPod("a", condition = unschedulable(NOW.minus(Duration.ofMinutes(3))))

    val state =
      WorkloadSchedulingState()
        .after(scanPods(listOf(podA), NOW), NOW)
        .after(scanPods(listOf(podA), NOW.plusSeconds(60)), NOW.plusSeconds(60))
        .after(scanPods(listOf(podA), NOW.plus(Duration.ofHours(3))), NOW.plus(Duration.ofHours(3)))

    assertEquals(ProblemRetention.Ongoing(SchedulingProblem("uid-a", NOW, "Unschedulable", MEMORY_MESSAGE)), state.problem)
  }

  @Test
  fun `a changed message for the same blocked pod updates the message but not the observation time`() {
    val since = NOW.minus(Duration.ofMinutes(3))
    val later = NOW.plusSeconds(60)

    val state =
      WorkloadSchedulingState()
        .after(scanPods(listOf(workloadPod("a", condition = unschedulable(since))), NOW), NOW)
        .after(scanPods(listOf(workloadPod("a", condition = unschedulable(since, message = "0/4 nodes are available."))), later), later)

    assertEquals(ProblemRetention.Ongoing(SchedulingProblem("uid-a", NOW, "Unschedulable", "0/4 nodes are available.")), state.problem)
  }

  @Test
  fun `a newly blocked pod replaces the problem with its own observation time`() {
    val podA = workloadPod("a", condition = unschedulable(NOW.minus(Duration.ofMinutes(10))))
    val podB = workloadPod("b", condition = unschedulable(NOW.minus(Duration.ofMinutes(3)), message = "0/3 nodes are available: 3 Insufficient cpu."))
    val later = NOW.plusSeconds(60)

    val state =
      WorkloadSchedulingState()
        .after(scanPods(listOf(podA), NOW), NOW)
        .after(scanPods(listOf(podA, podB), later), later)

    assertEquals(
      ProblemRetention.Ongoing(SchedulingProblem("uid-b", later, "Unschedulable", "0/3 nodes are available: 3 Insufficient cpu.")),
      state.problem,
    )
  }

  @Test
  fun `when several pods are newly blocked, the most recently blocked one is the problem`() {
    val pods =
      listOf(
        workloadPod("older", condition = unschedulable(NOW.minus(Duration.ofMinutes(5)), message = "older")),
        workloadPod("newer", condition = unschedulable(NOW.minus(Duration.ofMinutes(3)), message = "newer")),
      )

    val state = WorkloadSchedulingState().after(scanPods(pods, NOW), NOW)

    assertEquals(ProblemRetention.Ongoing(SchedulingProblem("uid-newer", NOW, "Unschedulable", "newer")), state.problem)
  }

  @Test
  fun `the problem expires one hour after the first clean scan, and later clean scans don't extend it`() {
    val problem = SchedulingProblem("uid-a", NOW, "Unschedulable", MEMORY_MESSAGE)
    val recoveredAt = NOW.plusSeconds(60)
    val beforeRecovery = WorkloadSchedulingState().after(scanPods(listOf(blockedPod("a")), NOW), NOW)

    val recovering =
      beforeRecovery
        .after(scanPods(emptyList(), recoveredAt), recoveredAt)
        .after(scanPods(emptyList(), recoveredAt.plus(Duration.ofMinutes(30))), recoveredAt.plus(Duration.ofMinutes(30)))
    assertEquals(ProblemRetention.Recovering(problem, recoveredAt), recovering.problem)

    val justBefore = recoveredAt.plus(PROBLEM_RETENTION_AFTER_RECOVERY).minusMillis(1)
    assertEquals(problem, recovering.after(scanPods(emptyList(), justBefore), justBefore).retainedProblem)

    val atExpiry = recoveredAt.plus(PROBLEM_RETENTION_AFTER_RECOVERY)
    assertEquals(ProblemRetention.None, recovering.after(scanPods(emptyList(), atExpiry), atExpiry).problem)
  }

  @Test
  fun `a problem during recovery replaces the old one and waits for a later clean scan to expire`() {
    val recoveredAt = NOW.plusSeconds(60)
    val reblockedAt = recoveredAt.plus(Duration.ofMinutes(10))
    val recoveredAgainAt = reblockedAt.plusSeconds(60)

    val state =
      WorkloadSchedulingState()
        .after(scanPods(listOf(blockedPod("a")), NOW), NOW)
        .after(scanPods(emptyList(), recoveredAt), recoveredAt)
        .after(scanPods(listOf(blockedPod("a")), reblockedAt), reblockedAt)

    val newProblem = SchedulingProblem("uid-a", reblockedAt, "Unschedulable", MEMORY_MESSAGE)
    assertEquals(ProblemRetention.Ongoing(newProblem), state.problem)

    val recoveredAgain = state.after(scanPods(emptyList(), recoveredAgainAt), recoveredAgainAt)
    val pastFirstExpiry = recoveredAt.plus(PROBLEM_RETENTION_AFTER_RECOVERY)
    assertEquals(newProblem, recoveredAgain.after(scanPods(emptyList(), pastFirstExpiry), pastFirstExpiry).retainedProblem)
  }

  private fun blockedPod(name: String) = workloadPod(name, condition = unschedulable(NOW.minus(Duration.ofMinutes(5))))

  private fun blocked(
    name: String,
    since: Instant,
    workloadType: String = "SYNC",
  ) = BlockedPod(uid = "uid-$name", name = name, workloadType = workloadType, blockedSince = since, message = MEMORY_MESSAGE)
}

internal fun workloadPod(
  name: String,
  jobType: String = "sync",
  phase: String = "Pending",
  createdAt: Instant = NOW.minus(Duration.ofHours(2)),
  deletedAt: Instant? = null,
  condition: PodCondition? = null,
): Pod =
  PodBuilder()
    .withNewMetadata()
    .withName(name)
    .withUid("uid-$name")
    .withCreationTimestamp(createdAt.toString())
    .withDeletionTimestamp(deletedAt?.toString())
    .withLabels<String, String>(mapOf("airbyte" to "job-pod", "job_type" to jobType))
    .endMetadata()
    .withNewStatus()
    .withPhase(phase)
    .withConditions(listOfNotNull(condition))
    .endStatus()
    .build()

internal fun unschedulable(
  since: Instant?,
  message: String = MEMORY_MESSAGE,
): PodCondition =
  PodConditionBuilder()
    .withType("PodScheduled")
    .withStatus("False")
    .withReason("Unschedulable")
    .withMessage(message)
    .withLastTransitionTime(since?.toString())
    .build()

internal fun scheduled(at: Instant?): PodCondition =
  PodConditionBuilder()
    .withType("PodScheduled")
    .withStatus("True")
    .withLastTransitionTime(at?.toString())
    .build()
