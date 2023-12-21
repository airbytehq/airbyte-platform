package io.airbyte.workload.handler

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.WorkloadStatus
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

  override fun workloadAlreadyExists(workloadId: String): Boolean {
    return workloadRepository.existsById(workloadId)
  }

  override fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    logPath: String,
    geography: String,
    mutexKey: String,
    type: WorkloadType,
  ) {
    val workloadAlreadyExists = workloadRepository.existsById(workloadId)
    if (workloadAlreadyExists) {
      throw ConflictException("Workload with id: $workloadId already exists")
    }
    val domainWorkload =
      DomainWorkload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = labels?.map { it.toDomain() },
        inputPayload = input,
        logPath = logPath,
        geography = geography,
        mutexKey = mutexKey,
        type = type.toDomain(),
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
      WorkloadStatus.PENDING -> workloadRepository.update(workloadId, dataplaneId, WorkloadStatus.CLAIMED)
      WorkloadStatus.CLAIMED -> {}
      else -> throw InvalidStatusTransitionException(
        "Tried to claim a workload that is not pending. Workload id: $workloadId has status: ${workload.status}",
      )
    }

    return true
  }

  override fun cancelWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.PENDING, WorkloadStatus.LAUNCHED, WorkloadStatus.CLAIMED, WorkloadStatus.RUNNING ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.CANCELLED,
          source,
          reason,
        )
      WorkloadStatus.CANCELLED -> logger.info { "Workload $workloadId is already cancelled. Cancelling an already cancelled workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Cannot cancel a workload in either success or failure status. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.FAILURE,
          source,
          reason,
        )
      WorkloadStatus.FAILURE -> logger.info { "Workload $workloadId is already marked as failed. Failing an already failed workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to fail a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun succeedWorkload(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.SUCCESS,
        )
      WorkloadStatus.SUCCESS ->
        logger.info { "Workload $workloadId is already marked as succeeded. Succeeding an already succeeded workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to succeed a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun setWorkloadStatusToRunning(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.RUNNING,
        )
      WorkloadStatus.RUNNING -> logger.info { "Workload $workloadId is already marked as running. Skipping..." }
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
    }
  }

  override fun setWorkloadStatusToLaunched(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.LAUNCHED,
        )
      WorkloadStatus.LAUNCHED -> logger.info { "Workload $workloadId is already marked as launched. Skipping..." }
      WorkloadStatus.RUNNING -> throw InvalidStatusTransitionException("Workload $workloadId is already marked as running. Skipping...")
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
    }
  }

  override fun heartbeat(workloadId: String) {
    val workload: DomainWorkload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING ->
        workloadRepository.update(
          workloadId,
          WorkloadStatus.RUNNING,
          OffsetDateTime.now(),
        )
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException("Heartbeat a non claimed workload")
    }
  }

  override fun getWorkloadsRunningCreatedBefore(
    dataplaneId: List<String>?,
    workloadType: List<ApiWorkloadType>?,
    createdBefore: OffsetDateTime?,
  ): List<Workload> {
    val domainWorkloads =
      workloadRepository.searchByTypeStatusAndCreationDate(
        dataplaneId,
        listOf(WorkloadStatus.RUNNING),
        workloadType?.map { it.toDomain() },
        createdBefore,
      )

    return domainWorkloads.map { it.toApi() }
  }
}
