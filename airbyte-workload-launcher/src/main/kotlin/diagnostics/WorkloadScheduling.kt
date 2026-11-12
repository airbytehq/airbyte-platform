/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.workers.pod.Metadata.JOB_TYPE_KEY
import io.fabric8.kubernetes.api.model.Pod
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * Time a pod may wait for a node before it counts as blocked, which leaves room for normal delays such as cluster autoscaling.
 */
internal val SCHEDULING_GRACE_PERIOD: Duration = Duration.ofMinutes(2)

/**
 * How long the last problem stays visible after the first scan that finds nothing blocked.
 */
internal val PROBLEM_RETENTION_AFTER_RECOVERY: Duration = Duration.ofHours(1)

// The Kubernetes message is free text from the scheduler and can list every node.
internal const val MAX_PROBLEM_MESSAGE_LENGTH = 500

private const val POD_SCHEDULED = "PodScheduled"
private const val UNSCHEDULABLE_REASON = "Unschedulable"
private val FINISHED_PHASES = setOf("Succeeded", "Failed")

internal data class ScheduledEvent(
  val scheduledAt: Instant,
  val workloadType: String,
)

internal data class BlockedPod(
  val uid: String,
  val name: String,
  val workloadType: String,
  val blockedSince: Instant,
  val message: String,
)

/**
 * A problem is one pod staying unschedulable. While that pod stays blocked, [message] follows Kubernetes but [observedAt] keeps
 * the scan that first found it, so a changed explanation is not a new problem.
 */
internal data class SchedulingProblem(
  val podUid: String,
  val observedAt: Instant,
  val reason: String,
  val message: String,
)

/**
 * What one completed scan found. [lastScheduled] is null when no pod has a scheduling time.
 */
internal data class PodScan(
  val blocked: List<BlockedPod>,
  val lastScheduled: ScheduledEvent?,
)

internal fun scanPods(
  pods: List<Pod>,
  scannedAt: Instant,
): PodScan =
  PodScan(
    blocked = pods.mapNotNull { blockedPod(it, scannedAt) },
    lastScheduled = pods.mapNotNull { scheduledEvent(it) }.maxByOrNull { it.scheduledAt },
  )

/**
 * Finished pods are left out: they no longer wait for a node. Being deleted counts as finished.
 */
private fun blockedPod(
  pod: Pod,
  scannedAt: Instant,
): BlockedPod? {
  if (pod.status?.phase in FINISHED_PHASES || pod.metadata.deletionTimestamp != null) return null
  val condition = pod.status?.conditions?.find { it.type == POD_SCHEDULED } ?: return null
  if (condition.status != "False" || condition.reason != UNSCHEDULABLE_REASON) return null

  val blockedSince = parseTime(condition.lastTransitionTime) ?: parseTime(pod.metadata.creationTimestamp) ?: return null
  if (Duration.between(blockedSince, scannedAt) <= SCHEDULING_GRACE_PERIOD) return null

  return BlockedPod(
    uid = pod.metadata.uid ?: pod.metadata.name,
    name = pod.metadata.name,
    workloadType = workloadType(pod),
    blockedSince = blockedSince,
    message = condition.message.orEmpty().take(MAX_PROBLEM_MESSAGE_LENGTH),
  )
}

/**
 * A finished pod still counts: it was scheduled. Without a transition time there is no event time to report.
 */
private fun scheduledEvent(pod: Pod): ScheduledEvent? {
  val condition = pod.status?.conditions?.find { it.type == POD_SCHEDULED && it.status == "True" } ?: return null
  val scheduledAt = parseTime(condition.lastTransitionTime) ?: return null
  return ScheduledEvent(scheduledAt = scheduledAt, workloadType = workloadType(pod))
}

// The pod lister selects pods by this label, so every scanned pod has it.
private fun workloadType(pod: Pod): String =
  pod.metadata.labels
    ?.get(JOB_TYPE_KEY)
    .orEmpty()
    .uppercase()

private fun parseTime(value: String?): Instant? =
  value?.let {
    try {
      Instant.parse(it)
    } catch (_: DateTimeParseException) {
      null
    }
  }

/**
 * The last scheduling problem, and whether the first scan after it found nothing blocked.
 */
internal sealed interface ProblemRetention {
  data object None : ProblemRetention

  data class Ongoing(
    val problem: SchedulingProblem,
  ) : ProblemRetention

  data class Recovering(
    val problem: SchedulingProblem,
    val recoveredAt: Instant,
  ) : ProblemRetention
}

/**
 * What the probe carries from one completed scan to the next. A scan that could not complete leaves it unchanged.
 */
internal data class WorkloadSchedulingState(
  val blockedPods: List<BlockedPod> = emptyList(),
  val lastScheduled: ScheduledEvent? = null,
  val problem: ProblemRetention = ProblemRetention.None,
) {
  /**
   * A pod that was not blocked in the previous scan is a new problem. When several are new, the one blocked most recently wins.
   */
  fun after(
    scan: PodScan,
    scannedAt: Instant,
  ): WorkloadSchedulingState {
    val previouslyBlocked = blockedPods.map { it.uid }.toSet()
    val newlyBlocked = scan.blocked.filter { it.uid !in previouslyBlocked }

    val problem =
      when {
        newlyBlocked.isNotEmpty() -> {
          val newest = newlyBlocked.maxWith(compareBy<BlockedPod> { it.blockedSince }.thenBy { it.name })
          ProblemRetention.Ongoing(SchedulingProblem(newest.uid, observedAt = scannedAt, reason = UNSCHEDULABLE_REASON, message = newest.message))
        }
        // Every blocked pod was blocked last time, so the problem is already ongoing.
        scan.blocked.isNotEmpty() -> withCurrentMessage(problem, scan.blocked)
        else -> recovered(problem, scannedAt)
      }

    return WorkloadSchedulingState(
      blockedPods = scan.blocked,
      lastScheduled = listOfNotNull(lastScheduled, scan.lastScheduled).maxByOrNull { it.scheduledAt },
      problem = problem,
    )
  }

  /**
   * The pod that has waited longest. Pods blocked since the same moment are ordered by name, so repeated scans pick the same one.
   */
  val longestBlocked: BlockedPod?
    get() = blockedPods.minWithOrNull(compareBy<BlockedPod> { it.blockedSince }.thenBy { it.name })

  val retainedProblem: SchedulingProblem?
    get() =
      when (val problem = problem) {
        ProblemRetention.None -> null
        is ProblemRetention.Ongoing -> problem.problem
        is ProblemRetention.Recovering -> problem.problem
      }
}

private fun withCurrentMessage(
  problem: ProblemRetention,
  blocked: List<BlockedPod>,
): ProblemRetention {
  if (problem !is ProblemRetention.Ongoing) return problem
  val pod = blocked.find { it.uid == problem.problem.podUid } ?: return problem
  return ProblemRetention.Ongoing(problem.problem.copy(message = pod.message))
}

private fun recovered(
  problem: ProblemRetention,
  scannedAt: Instant,
): ProblemRetention =
  when (problem) {
    ProblemRetention.None -> problem
    is ProblemRetention.Ongoing -> ProblemRetention.Recovering(problem.problem, recoveredAt = scannedAt)
    is ProblemRetention.Recovering ->
      if (Duration.between(problem.recoveredAt, scannedAt) >= PROBLEM_RETENTION_AFTER_RECOVERY) ProblemRetention.None else problem
  }
