/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import jakarta.inject.Singleton
import java.util.Locale
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
  fun generate(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    workloadType: WorkloadType,
  ): String {
    return "${connectionId}_${jobId}_${attemptNumber}_${workloadType.name.lowercase(Locale.ENGLISH)}"
  }
}

enum class WorkloadType {
  CHECK,
  DISCOVER,
  SYNC,
}
