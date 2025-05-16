/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
 *
 * IMPORTANT: Kube only accepts labels of up to 63 characters, so we try to keep these short for use with kube.
 * NOTE: UUIDs are 36 characters.
 */
@Singleton
@Deprecated("To delete in favor of io.airbyte.server.helpers.WorkloadIdGenerator once migrated to commands")
class WorkloadIdGenerator {
  fun generateCheckWorkloadId(
    actorDefinitionId: UUID,
    jobId: String,
    attemptNumber: Int,
  ): String = "${actorDefinitionId}_${jobId}_${attemptNumber}_check"

  fun generateDiscoverWorkloadId(
    actorDefinitionId: UUID,
    jobId: String,
    attemptNumber: Int,
  ): String = "${actorDefinitionId}_${jobId}_${attemptNumber}_discover"

  fun generateDiscoverWorkloadIdV2(
    actorId: UUID,
    timestampMs: Long,
  ): String = "${actorId}_${timestampMs}_discover"

  fun generateDiscoverWorkloadIdV2WithSnap(
    actorId: UUID,
    timestampMs: Long,
    windowWidthMs: Long,
  ): String {
    val snapped = timestampMs - (timestampMs % windowWidthMs)

    return generateDiscoverWorkloadIdV2(actorId, snapped)
  }

  fun generateSpecWorkloadId(differentiator: String): String = "${differentiator}_spec"

  fun generateSyncWorkloadId(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): String = "${connectionId}_${jobId}_${attemptNumber}_sync"
}
