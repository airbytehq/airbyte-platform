package io.airbyte.workload.handler

import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.NotModifiedException
import io.airbyte.workload.repository.WorkloadRepository
import jakarta.inject.Singleton
import jakarta.transaction.Transactional
import java.time.OffsetDateTime

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
open class WorkloadHandler(
  private val workloadRepository: WorkloadRepository,
) {
  open fun getWorkload(workloadId: String): ApiWorkload {
    return getDomainWorkload(workloadId).toApi()
  }

  private fun getDomainWorkload(workloadId: String): DomainWorkload {
    return workloadRepository.findById(workloadId)
      .orElseThrow { NotFoundException("Could not find workload with id: $workloadId") }
  }

  open fun getWorkloads(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    updatedBefore: OffsetDateTime?,
  ): List<Workload> {
    if (dataplaneId?.isEmpty() == true || workloadStatus?.isEmpty() == true) {
      return emptyList()
    }

    val domainWorkloads =
      workloadRepository.search(
        dataplaneId,
        workloadStatus?.map { it.toDomain() },
        updatedBefore,
      )

    return domainWorkloads.map { it.toApi() }
  }

  @Transactional
  open fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
  ) {
    val workloadAlreadyExists = workloadRepository.existsById(workloadId)
    if (workloadAlreadyExists) {
      throw NotModifiedException("Workload with id: $workloadId already exists")
    }
    val domainWorkload =
      DomainWorkload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.pending,
        lastHeartbeatAt = null,
        workloadLabels = labels?.map { it.toDomain() },
      )

    workloadRepository.save(domainWorkload).toApi()
  }

  open fun updateWorkload(
    workloadId: String,
    workloadStatus: ApiWorkloadStatus,
  ) {
    workloadRepository.update(workloadId, workloadStatus.toDomain())
  }

  @Transactional
  open fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
  ): Boolean {
    val workload = getDomainWorkload(workloadId)

    if (workload.dataplaneId != null) {
      return false
    }

    when (workload.status) {
      WorkloadStatus.pending -> workloadRepository.update(workloadId, dataplaneId, WorkloadStatus.claimed)
      else -> throw InvalidStatusTransitionException(
        String.format("Tried to claim a workload that is not pending. Workload id: %s has status: %s", workloadId, workload.status),
      )
    }

    return true
  }

  @Transactional
  open fun heartbeat(workloadId: String) {
    val workload: DomainWorkload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.claimed, WorkloadStatus.running ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.running,
          OffsetDateTime.now(),
        )
      WorkloadStatus.cancelled, WorkloadStatus.failure, WorkloadStatus.success -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.pending -> throw InvalidStatusTransitionException("Heartbeat a non claimed workload")
    }
  }
}
