/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import jakarta.inject.Singleton
import java.util.UUID

/**
 * Generates a Workload ID using the following format:
 * <p>
 *     <code>&lt;the connection ID&gtl;_&lt;the job ID&gt;_&lt;the job attempt number&gt;_&lt;the workload job type&gt;</code>
 * </p>
 * where the workload type is a value defined in {@link WorkloadType}.
 */
@Singleton
class WorkloadIdGenerator {
  fun generateCheckWorkloadId(
    actorId: UUID,
    jobId: String,
    attemptNumber: Int,
  ): String {
    return "${actorId}_${jobId}_${attemptNumber}_check"
  }

  fun generateDiscoverWorkloadId(
    actorId: UUID,
    differentiator: UUID,
  ): String {
    return "${actorId}_${differentiator}_discover"
  }

  fun generateSpeckWorkloadId(
    workspaceId: UUID,
    differentiator: UUID,
  ): String {
    return "${workspaceId}_${differentiator}_spec"
  }

  fun generateSyncWorkloadId(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): String {
    return "${connectionId}_${jobId}_${attemptNumber}_sync"
  }
}
