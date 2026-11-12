/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.domain.models.DataplaneDiagnosticKind
import io.airbyte.domain.models.DataplaneDiagnosticOutcome
import java.util.UUID

/**
 * Returns why a reported result can't be saved, or an empty list when it can. The health read hands the observation to the
 * webapp as saved, so a malformed one is rejected here rather than shown.
 */
internal fun diagnosticObservationProblems(
  kind: DataplaneDiagnosticKind,
  outcome: DataplaneDiagnosticOutcome,
  targetId: UUID,
  dataplaneId: UUID,
  observation: Map<String, Any?>,
): List<String> =
  when (kind) {
    DataplaneDiagnosticKind.SECRET_STORAGE ->
      listOf("scope_type", "scope_id").filter { it !in observation }.map { "observation is missing $it" }
    DataplaneDiagnosticKind.STORAGE_REACHABILITY -> emptyList()
    DataplaneDiagnosticKind.WORKLOAD_SCHEDULING -> workloadSchedulingProblems(outcome, targetId, dataplaneId, observation)
  }

/**
 * Times in the observation are epoch milliseconds, like every other time in the data plane health API, so an ISO string is
 * rejected. A field not observed yet may be left out, which reads the same as null.
 */
private fun workloadSchedulingProblems(
  outcome: DataplaneDiagnosticOutcome,
  targetId: UUID,
  dataplaneId: UUID,
  observation: Map<String, Any?>,
): List<String> {
  if (targetId != dataplaneId) {
    return listOf("target_id must be the data plane id")
  }

  return countProblems(outcome, observation) +
    eventProblems(observation, "last_scheduled", timeKey = "scheduled_at", textKeys = listOf("workload_type")) +
    eventProblems(observation, "last_problem", timeKey = "observed_at", textKeys = listOf("reason", "message"))
}

/**
 * The counts are what the scan measured. A scan that could not complete measured nothing, so it must leave them out rather than
 * repeat an earlier scan's counts as current.
 */
private fun countProblems(
  outcome: DataplaneDiagnosticOutcome,
  observation: Map<String, Any?>,
): List<String> {
  val blockedCount = observation["blocked_count"]
  val longestBlockedSince = observation["longest_blocked_since"]
  val longestBlockedType = observation["longest_blocked_workload_type"]
  if (outcome is DataplaneDiagnosticOutcome.UnableToComplete) {
    return if (blockedCount == null && longestBlockedSince == null && longestBlockedType == null) {
      emptyList()
    } else {
      listOf("an incomplete scan must leave out blocked_count, longest_blocked_since and longest_blocked_workload_type")
    }
  }

  if (blockedCount !is Int || blockedCount < 0) {
    return listOf("blocked_count must be a non-negative integer")
  }

  return buildList {
    if (outcome == DataplaneDiagnosticOutcome.Passed && blockedCount != 0) add("a passed scan must have blocked_count 0")
    if (outcome is DataplaneDiagnosticOutcome.Failed && blockedCount == 0) add("a failed scan must have a positive blocked_count")
    if (blockedCount == 0 && longestBlockedSince != null) add("longest_blocked_since must be left out when nothing is blocked")
    if (blockedCount > 0 && !isEpochMillis(longestBlockedSince)) add("longest_blocked_since must be epoch milliseconds")
    if (blockedCount == 0 && longestBlockedType != null) add("longest_blocked_workload_type must be left out when nothing is blocked")
    if (blockedCount > 0 && longestBlockedType !is String) add("longest_blocked_workload_type must be a string")
  }
}

private fun eventProblems(
  observation: Map<String, Any?>,
  eventKey: String,
  timeKey: String,
  textKeys: List<String>,
): List<String> {
  val event = observation[eventKey] ?: return emptyList()
  if (event !is Map<*, *>) {
    return listOf("$eventKey must be an object or null")
  }

  return buildList {
    if (!isEpochMillis(event[timeKey])) add("$eventKey.$timeKey must be epoch milliseconds")
    textKeys.filter { event[it] !is String }.forEach { add("$eventKey.$it must be a string") }
  }
}

// Jackson reads a JSON integer as Int or Long depending on its size, and a JSON number with a fraction as Double.
private fun isEpochMillis(value: Any?): Boolean = (value is Long || value is Int) && (value as Number).toLong() > 0
