/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import jakarta.inject.Singleton
import java.util.UUID
import kotlin.time.Duration

/**
 * Generates a Workload ID using the following format:
 * <p>
 *     <code>&lt;the connection ID&gtl;_&lt;the job ID&gt;_&lt;the job attempt number&gt;_&lt;the workload job type&gt;</code>
 * </p>
 * where the workload type is a value defined in {@link WorkloadType}.
 *
 * IMPORTANT: Kube only accepts labels of up to 63 characters, so we try to keep these short for use with kube.
 * NOTE: UUIDs are 36 characters.
 */
@Singleton
class WorkloadIdGenerator {
  // TODO delete the old version io.airbyte.workers.workload.WorkloadIdGenerator once migrated to commands

  fun generateCheckWorkloadId(
    actorId: UUID?,
    actorDefinitionId: UUID,
    jobId: String,
    attemptNumber: Long,
  ): String = "${actorId ?: actorDefinitionId}_${jobId}_${attemptNumber}_check"

  fun generateDiscoverWorkloadId(
    actorId: UUID,
    jobId: String,
    attemptNumber: Long,
    isManual: Boolean,
    discoverAutoRefreshWindow: Duration,
  ): String {
    if (isManual) {
      return "${actorId}_${jobId}_${attemptNumber}_discover"
    } else {
      val timestampMs = System.currentTimeMillis()
      val snapped = timestampMs - (timestampMs % discoverAutoRefreshWindow.inWholeMilliseconds)
      return "${actorId}_${snapped}_discover"
    }
  }

  fun generateReplicateWorkloadId(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Long,
  ): String = "${connectionId}_${jobId}_${attemptNumber}_sync"
}
