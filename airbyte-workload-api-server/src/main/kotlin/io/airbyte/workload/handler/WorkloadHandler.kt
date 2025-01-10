package io.airbyte.workload.handler

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import jakarta.transaction.Transactional
import java.time.OffsetDateTime
import java.util.UUID

/**
 * In order to mock a class it needs to be open. We have added this interface to avoid making [WorkloadHandler] an open class.
 * There is a chance that once we move to micronaut 4 this issue will be resolved, and we can remove this interface.
 */
@Transactional
interface WorkloadHandler {
  fun getWorkload(workloadId: String): ApiWorkload

  fun getWorkloads(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    updatedBefore: OffsetDateTime?,
  ): List<Workload>

  fun getWorkloadsWithExpiredDeadline(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    deadline: OffsetDateTime,
  ): List<Workload>

  fun workloadAlreadyExists(workloadId: String): Boolean

  fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    logPath: String,
    mutexKey: String?,
    type: WorkloadType,
    autoId: UUID,
    deadline: OffsetDateTime,
    signalInput: String?,
  )

  fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
    deadline: OffsetDateTime,
  ): Boolean

  fun cancelWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  )

  fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  )

  fun succeedWorkload(workloadId: String)

  fun setWorkloadStatusToRunning(
    workloadId: String,
    deadline: OffsetDateTime,
  )

  fun setWorkloadStatusToLaunched(
    workloadId: String,
    deadline: OffsetDateTime,
  )

  fun heartbeat(
    workloadId: String,
    deadline: OffsetDateTime,
  )

  fun getWorkloadsRunningCreatedBefore(
    dataplaneId: List<String>?,
    workloadType: List<ApiWorkloadType>?,
    createdBefore: OffsetDateTime?,
  ): List<Workload>
}
