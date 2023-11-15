package io.airbyte.workload.handler

import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import jakarta.transaction.Transactional
import java.time.OffsetDateTime

/**
 * In order to mock a class it needs to be open. We have added this interface to avoid making [WorkloadHandler] an open class.
 * There is a chance that once we move to micronaut 4 this issue will be resolved, and we can remove this interface.
 */
interface WorkloadHandler {
  fun getWorkload(workloadId: String): ApiWorkload

  fun getWorkloads(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    updatedBefore: OffsetDateTime?,
  ): List<Workload>

  @Transactional
  fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    logPath: String,
  )

  @Transactional
  fun updateWorkload(
    workloadId: String,
    workloadStatus: ApiWorkloadStatus,
  )

  @Transactional
  fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
  ): Boolean

  @Transactional
  fun cancelWorkload(workloadId: String)

  @Transactional
  fun heartbeat(workloadId: String)
}
