package io.airbyte.workload.handler

import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.NotModifiedException
import io.airbyte.workload.repository.WorkloadRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime

private val logger = KotlinLogging.logger {}

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
class WorkloadHandlerImpl(
  private val workloadRepository: WorkloadRepository,
) : WorkloadHandler {
  override fun getWorkload(workloadId: String): ApiWorkload {
    return getDomainWorkload(workloadId).toApi()
  }

  private fun getDomainWorkload(workloadId: String): DomainWorkload {
    return workloadRepository.findById(workloadId)
      .orElseThrow { NotFoundException("Could not find workload with id: $workloadId") }
  }

  override fun getWorkloads(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    updatedBefore: OffsetDateTime?,
  ): List<Workload> {
    val domainWorkloads =
      workloadRepository.search(
        dataplaneId,
        workloadStatus?.map { it.toDomain() },
        updatedBefore,
      )

    return domainWorkloads.map { it.toApi() }
  }

  override fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    logPath: String,
    geography: String,
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
        inputPayload = input,
        logPath = logPath,
        geography = geography,
      )

    workloadRepository.save(domainWorkload).toApi()
  }

  override fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
  ): Boolean {
    val workload = getDomainWorkload(workloadId)

    if (workload.dataplaneId != null && !workload.dataplaneId.equals(dataplaneId)) {
      return false
    }

    when (workload.status) {
      WorkloadStatus.pending -> workloadRepository.update(workloadId, dataplaneId, WorkloadStatus.claimed)
      WorkloadStatus.claimed -> {}
      else -> throw InvalidStatusTransitionException(
        "Tried to claim a workload that is not pending. Workload id: $workloadId has status: ${workload.status}",
      )
    }

    return true
  }

  override fun cancelWorkload(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.pending, WorkloadStatus.running, WorkloadStatus.claimed ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.cancelled,
        )
      WorkloadStatus.cancelled -> logger.info { "Workload $workloadId is already cancelled. Cancelling an already cancelled workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Cannot cancel a workload in either success or failure status. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun failWorkload(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.claimed, WorkloadStatus.running ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.failure,
        )
      WorkloadStatus.failure -> logger.info { "Workload $workloadId is already marked as failed. Failing an already failed workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to fail a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun succeedWorkload(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.claimed, WorkloadStatus.running ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.success,
        )
      WorkloadStatus.success ->
        logger.info { "Workload $workloadId is already marked as succeeded. Succeeding an already succeeded workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to succeed a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun setWorkloadStatusToRunning(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.claimed ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.success,
        )
      WorkloadStatus.running -> logger.info { "Workload $workloadId is already marked as running. Trying to update its status to running is a noop" }
      WorkloadStatus.cancelled, WorkloadStatus.failure, WorkloadStatus.success -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.pending -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
    }
  }

  override fun heartbeat(workloadId: String) {
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
