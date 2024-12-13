package io.airbyte.workload.handler

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.SignalInput
import io.airbyte.commons.json.Jsons
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.metrics.CustomMetricPublisher
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID
import io.airbyte.workload.repository.domain.WorkloadType as DomainWorkloadType

private val logger = KotlinLogging.logger {}

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
class WorkloadHandlerImpl(
  private val workloadRepository: WorkloadRepository,
  private val airbyteApi: AirbyteApiClient,
  private val metricClient: CustomMetricPublisher,
) : WorkloadHandler {
  companion object {
    val ACTIVE_STATUSES: List<WorkloadStatus> =
      listOf(WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING)
  }

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
    mutexKey: String?,
    type: WorkloadType,
    autoId: UUID,
    deadline: OffsetDateTime,
    signalInput: String?,
  ) {
    val workloadAlreadyExists = workloadRepository.existsById(workloadId)
    if (workloadAlreadyExists) {
      throw ConflictException("Workload with id: $workloadId already exists")
    }

    // Create the workload and then check for mutexKey uniqueness.
    // This will lead to a more deterministic concurrency resolution in the event of concurrent create calls.
    val domainWorkload =
      DomainWorkload(
        id = workloadId,
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = labels?.map { it.toDomain() },
        inputPayload = input,
        logPath = logPath,
        mutexKey = mutexKey,
        type = type.toDomain(),
        autoId = autoId,
        deadline = deadline,
        signalInput = signalInput,
      )
    workloadRepository.save(domainWorkload).toApi()

    // Evaluating feature flag with UUID_ZERO because the client requires a context. This feature flag is intended to be used
    // as a global kill switch for validation.
    if (mutexKey != null) {
      // Keep the most recent workload by creation date with mutexKey, fail the others.
      workloadRepository
        .searchByMutexKeyAndStatusInList(mutexKey, statuses = ACTIVE_STATUSES)
        .sortedByDescending { it.createdAt }
        .drop(1)
        .forEach {
          try {
            logger.info { "${it.id} violates the $mutexKey uniqueness constraint, failing in favor of $workloadId before continuing." }
            failWorkload(it.id, source = "workload-api", reason = "Superseded by $workloadId")
          } catch (_: InvalidStatusTransitionException) {
            // This edge case happens if the workload reached a terminal state through another path.
            // This would be unusual but not actionable, so we're logging a message rather than failing the call.
            logger.info { "${it.id} was completed before being superseded by $workloadId" }
          }
        }
    }
  }

  override fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
    deadline: OffsetDateTime,
  ): Boolean {
    val workload = getDomainWorkload(workloadId)

    if (workload.dataplaneId != null && !workload.dataplaneId.equals(dataplaneId)) {
      return false
    }

    when (workload.status) {
      WorkloadStatus.PENDING ->
        workloadRepository.update(
          workloadId,
          dataplaneId,
          WorkloadStatus.CLAIMED,
          deadline,
        )
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
      WorkloadStatus.PENDING, WorkloadStatus.LAUNCHED, WorkloadStatus.CLAIMED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.CANCELLED,
          source,
          reason,
          null,
        )
        sendSignal(workload.type, workload.signalInput)
      }
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
      WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.FAILURE,
          source,
          reason,
          null,
        )
        sendSignal(workload.type, workload.signalInput)
      }
      WorkloadStatus.FAILURE -> logger.info { "Workload $workloadId is already marked as failed. Failing an already failed workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to fail a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun succeedWorkload(workloadId: String) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.SUCCESS,
          null,
        )
        sendSignal(workload.type, workload.signalInput)
      }
      WorkloadStatus.SUCCESS ->
        logger.info { "Workload $workloadId is already marked as succeeded. Succeeding an already succeeded workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to succeed a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  override fun setWorkloadStatusToRunning(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.RUNNING,
          deadline,
        )
      }
      WorkloadStatus.RUNNING -> logger.info { "Workload $workloadId is already marked as running. Skipping..." }
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
    }
  }

  override fun setWorkloadStatusToLaunched(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.LAUNCHED,
          deadline,
        )
      }
      WorkloadStatus.LAUNCHED -> logger.info { "Workload $workloadId is already marked as launched. Skipping..." }
      WorkloadStatus.RUNNING -> logger.info { "Workload $workloadId is already marked as running. Skipping..." }
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
    }
  }

  override fun heartbeat(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload: DomainWorkload = getDomainWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.RUNNING,
          offsetDateTime(),
          deadline,
        )
      }
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Heartbeat a workload in a terminal state",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException("Heartbeat a non claimed workload")
    }
  }

  fun offsetDateTime(): OffsetDateTime = OffsetDateTime.now()

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

  override fun getWorkloadsWithExpiredDeadline(
    dataplaneId: List<String>?,
    workloadStatus: List<ApiWorkloadStatus>?,
    deadline: OffsetDateTime,
  ): List<Workload> {
    val domainWorkloads =
      workloadRepository.searchForExpiredWorkloads(
        dataplaneId,
        workloadStatus?.map { it.toDomain() },
        deadline,
      )

    return domainWorkloads.map { it.toApi() }
  }

  private fun sendSignal(
    workloadType: DomainWorkloadType,
    signalPayload: String?,
  ) {
    val signalInput =
      if (signalPayload == null) {
        null
      } else {
        try {
          Jsons.deserialize(signalPayload, io.airbyte.config.SignalInput::class.java)
        } catch (e: Exception) {
          logger.error(e) { "Failed to deserialize signal payload: $signalPayload" }
          metricClient.count(
            OssMetricsRegistry.WORKLOADS_SIGNAL.metricName,
            MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
            MetricAttribute(MetricTags.FAILURE_TYPE, "deserialization"),
          )
          return
        }
      }
    if (signalInput != null) {
      try {
        airbyteApi.signalApi.signal(
          SignalInput(
            workflowType = signalInput.workflowType,
            workflowId = signalInput.workflowId,
          ),
        )
        metricClient.count(
          OssMetricsRegistry.WORKLOADS_SIGNAL.metricName,
          MetricAttribute(MetricTags.WORKFLOW_TYPE, signalInput.workflowType),
          MetricAttribute(MetricTags.WORKLOAD_TYPE, workloadType.toString()),
          MetricAttribute(MetricTags.STATUS, MetricTags.SUCCESS),
        )
      } catch (e: Exception) {
        logger.error(e) { "Failed to send signal for the payload: $signalPayload" }
        metricClient.count(
          OssMetricsRegistry.WORKLOADS_SIGNAL.metricName,
          MetricAttribute(MetricTags.WORKFLOW_TYPE, signalInput.workflowType),
          MetricAttribute(MetricTags.WORKLOAD_TYPE, workloadType.toString()),
          MetricAttribute(MetricTags.STATUS, MetricTags.FAILURE),
          MetricAttribute(MetricTags.FAILURE_TYPE, e.message),
        )
      }
    }
  }
}
