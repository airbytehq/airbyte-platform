/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.WorkloadType
import java.time.Duration

/**
 * Runs one type of direct job: a workload the launcher hands to this runner instead of launching a pod.
 *
 * [DirectJobService] owns everything shared by direct jobs (dedupe, the status guard, marking the workload running,
 * and reporting success or failure). A handler only parses its input, does the work, and writes its output.
 */
interface DirectJobHandler {
  val workloadType: WorkloadType

  /** How long the workload may stay RUNNING before the workload API times it out. */
  val runningDeadline: Duration

  /**
   * Parses [inputPayload], runs the job, and writes its output for [workloadId].
   *
   * A job whose outcome is a normal negative result (for example, an unreachable host) must write that result and
   * return. Throw only for platform errors; the workload is then failed with an `airbyte_platform` origin.
   */
  fun run(
    workloadId: String,
    inputPayload: String,
  )
}
