/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.services

import io.airbyte.commons.enums.convertTo
import io.airbyte.config.WorkloadConstants.Companion.LAUNCH_ERROR_SOURCE
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.DisableWorkloadLabelTableWrite
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseWorkloadLabelsJsonbOnly
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.common.DefaultDeadlineValues
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.signal.SignalSender
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private val logger = KotlinLogging.logger {}

sealed class WorkloadServiceException(
  message: String,
) : Exception(message)

class ConflictException(
  message: String,
) : WorkloadServiceException(message)

class InvalidStatusTransitionException(
  message: String,
) : WorkloadServiceException(message)

class NotFoundException(
  message: String,
) : WorkloadServiceException(message)

@Singleton
class WorkloadService(
  private val workloadRepository: WorkloadRepository,
  private val workloadQueueRepository: WorkloadQueueRepository,
  private val signalSender: SignalSender,
  private val defaultDeadlineValues: DefaultDeadlineValues,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    workspaceId: UUID?,
    organizationId: UUID?,
    logPath: String,
    mutexKey: String?,
    type: WorkloadType,
    autoId: UUID,
    deadline: OffsetDateTime?,
    signalInput: String?,
    dataplaneGroup: String?,
    priority: WorkloadPriority?,
  ): Workload {
    val workloadAlreadyExists = workloadRepository.existsById(workloadId)
    if (workloadAlreadyExists) {
      throw ConflictException("Workload with id: $workloadId already exists")
    }

    // Convert List<WorkloadLabel> to Map<String, String> for JSONB column
    val labelsMap = labels?.associate { it.key to it.value }

    // Check feature flag to determine if we should write to legacy table
    val disableLegacyWrite = featureFlagClient.boolVariation(DisableWorkloadLabelTableWrite, Empty)

    // Create the workload and then check for mutexKey uniqueness.
    // This will lead to a more deterministic concurrency resolution in the event of concurrent create calls.
    val savedWorkload =
      workloadRepository.save(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.PENDING,
          workloadLabels = if (disableLegacyWrite) null else labels, // Conditionally write to legacy table
          labels = labelsMap, // Write to labels JSONB column (new)
          inputPayload = input,
          workspaceId = workspaceId,
          organizationId = organizationId,
          logPath = logPath,
          mutexKey = mutexKey,
          type = type.convertTo(),
          autoId = autoId,
          deadline = deadline ?: defaultDeadlineValues.createStepDeadline(),
          signalInput = signalInput,
          dataplaneGroup = dataplaneGroup,
          priority = priority?.toInt() ?: 0,
        ),
      )

    if (mutexKey != null) {
      // Keep the most recent workload by creation date with mutexKey, fail the others.
      workloadRepository
        .searchByMutexKeyAndStatusInList(mutexKey, statuses = ACTIVE_STATUSES)
        .sortedByDescending { it.createdAt }
        .drop(1)
        .forEach {
          try {
            logger.info { "${it.id} violates the $mutexKey uniqueness constraint, failing in favor of $workloadId before continuing." }
            // dataplaneVersion is null in this context because a createWorkload call doesn't come from a dataplane.
            failWorkload(it.id, source = "workload-api", reason = "Superseded by $workloadId", dataplaneVersion = null)
          } catch (_: InvalidStatusTransitionException) {
            // This edge case happens if the workload reached a terminal state through another path.
            // This would be unusual but not actionable, so we're logging a message rather than failing the call.
            logger.info { "${it.id} was completed before being superseded by $workloadId" }
          }
        }
    }

    return savedWorkload
  }

  fun cancelWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    val workload = workloadRepository.cancel(workloadId, reason = reason, source = source)
    if (workload != null) {
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
      signalSender.sendSignal(workload.type, workload.signalInput)
    } else {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Cannot cancel a workload in either success or failure status. Workload id: $workloadId has status: ${w.status}",
            )
            WorkloadStatus.CANCELLED ->
              logger.info {
                "Workload $workloadId is already cancelled. Cancelling an already cancelled workload is a noop"
              }
            else -> logger.error { "Failed to update workload ($workloadId) to cancelled. Current status is ${w.status}." }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  /**
   * Claim a workload for [dataplaneId].
   *
   * Returns the workload if the claim was successful, null otherwise.
   */
  fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ): Workload? {
    val workload = workloadRepository.claim(workloadId, dataplaneId, deadline)
    if (workload != null) {
      val claimed = workload.status == WorkloadStatus.CLAIMED && workload.dataplaneId == dataplaneId
      if (claimed) {
        emitTimeToTransitionMetric(workload, WorkloadStatus.CLAIMED, dataplaneVersion)
        workloadQueueRepository.ackWorkloadQueueItem(workloadId)
        return workload
      }
    }
    return null
  }

  fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
    dataplaneVersion: String?,
  ) {
    val workload = workloadRepository.fail(workloadId, reason = reason, source = source)
    if (workload != null) {
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
      emitTimeToTransitionMetric(workload, WorkloadStatus.FAILURE, dataplaneVersion)
      if (source == LAUNCH_ERROR_SOURCE) {
        emitWorkloadLaunchMetric(workload, MetricTags.FAILURE, dataplaneVersion)
      }
      signalSender.sendSignal(workload.type, workload.signalInput)
    } else {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Cannot fail a workload in either canceled or success status. Workload id: $workloadId has status: ${w.status}",
            )
            WorkloadStatus.FAILURE ->
              logger.info {
                "Workload $workloadId is already failed. Failing an already failed workload is a noop"
              }
            else -> logger.error { "Failed to update workload ($workloadId) to failed. Current status is ${w.status}." }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun launchWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    val workload =
      workloadRepository.launch(workloadId, deadline)
        ?: workloadRepository.findById(workloadId).orElseThrow { NotFoundException("Workload $workloadId not found") }

    // Always emit this metric. Even though the workload state transition may have failed because the workload was already in a further state,
    // this reflects when the launcher finished launching.
    emitTimeToTransitionMetric(workload, WorkloadStatus.LAUNCHED, dataplaneVersion)
    emitWorkloadLaunchMetric(workload, MetricTags.SUCCESS, dataplaneVersion)

    when (workload.status) {
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Trying to set a workload in a terminal state (${workload.status}) to launched",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
      else -> logger.error { "Failed to update workload ($workloadId) to launched. Current status is ${workload.status}." }
    }
  }

  fun heartbeatWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    // Heartbeat only updates timestamps, does NOT change status
    // Callers must call runningWorkload() first to transition to running state
    val rowsAffected = workloadRepository.heartbeat(workloadId, deadline)
    if (rowsAffected == 0) {
      metricClient.count(
        OssMetricsRegistry.WORKLOAD_HEARTBEAT,
        1L,
        MetricAttribute(MetricTags.DATA_PLANE_VERSION, dataplaneVersion ?: MetricTags.UNKNOWN),
        MetricAttribute(MetricTags.STATUS_TAG, MetricTags.FAILURE),
      )
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Heartbeat a workload in a terminal state (${w.status})",
            )
            WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
              "Heartbeat a workload that is not claimed.",
            )
            WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED -> throw InvalidStatusTransitionException(
              "Heartbeat a workload that is not running (${w.status}). /running must be called before heartbeat.",
            )
            else -> logger.error { "Failed to heartbeat workload ($workloadId). Current status is ${w.status}." }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    } else {
      metricClient.count(
        OssMetricsRegistry.WORKLOAD_HEARTBEAT,
        1L,
        MetricAttribute(MetricTags.DATA_PLANE_VERSION, dataplaneVersion ?: MetricTags.UNKNOWN),
        MetricAttribute(MetricTags.STATUS_TAG, MetricTags.SUCCESS),
      )
    }
  }

  fun runningWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    val workload =
      workloadRepository.running(workloadId, deadline)
        ?: workloadRepository.findById(workloadId).orElseThrow { NotFoundException("Workload $workloadId not found") }

    // Always emit this metric. Even though the workload state transition may have failed because the workload was already in a further state,
    // this reflects when the workload reported it was up and running.
    emitTimeToTransitionMetric(workload, WorkloadStatus.RUNNING, dataplaneVersion)

    when (workload.status) {
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
        "Trying to set a workload in a terminal state (${workload.status}) to running",
      )
      WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
        "Can't set a workload status to running on a workload that hasn't been claimed",
      )
      else -> logger.error { "Failed to update workload ($workloadId) to running. Current status is ${workload.status}." }
    }
  }

  fun succeedWorkload(
    workloadId: String,
    dataplaneVersion: String?,
  ) {
    val workload = workloadRepository.succeed(workloadId)
    if (workload != null) {
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
      emitTimeToTransitionMetric(workload, WorkloadStatus.SUCCESS, dataplaneVersion)
      signalSender.sendSignal(workload.type, workload.signalInput)
    } else {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE -> throw InvalidStatusTransitionException(
              "Cannot fail a workload in either canceled or failure status. Workload id: $workloadId has status: ${w.status}",
            )

            WorkloadStatus.SUCCESS ->
              logger.info {
                "Workload $workloadId is already successful. Succeeding an already successful workload is a noop"
              }

            else -> logger.error { "Failed to update workload ($workloadId) to success. Current status is ${w.status}." }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun getWorkload(workloadId: String): Workload {
    val useJsonbLabelsOnly = featureFlagClient.boolVariation(UseWorkloadLabelsJsonbOnly, Empty)
    return if (useJsonbLabelsOnly) {
      workloadRepository.findByIdWithoutLegacyLabels(workloadId).orElseThrow {
        NotFoundException("Could not find workload with id: $workloadId")
      }
    } else {
      workloadRepository.findById(workloadId).orElseThrow {
        NotFoundException("Could not find workload with id: $workloadId")
      }
    }
  }

  /**
   * Get all non-terminal workloads for a specific connection.
   * Used for administrative operations like force cleanup.
   *
   * Filters at the database level using both the legacy workloadLabels table
   * and the new labels JSONB column for compatibility during migration.
   */
  fun getNonTerminalWorkloadsByConnection(connectionId: UUID): List<Workload> =
    workloadRepository.findByConnectionIdAndStatuses(
      connectionId = connectionId.toString(),
      statuses = ACTIVE_STATUSES,
    )

  private fun emitTimeToTransitionMetric(
    workload: Workload,
    status: WorkloadStatus,
    dataplaneVersion: String?,
  ) {
    workload.timeSinceCreateInMillis()?.let {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_TIME_TO_TRANSITION_FROM_CREATE,
        it.toDouble(),
        MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workload.type.name),
        MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, workload.dataplaneGroup ?: MetricTags.UNKNOWN),
        MetricAttribute(MetricTags.DATA_PLANE_ID_TAG, workload.dataplaneId ?: MetricTags.UNKNOWN),
        MetricAttribute(MetricTags.DATA_PLANE_VERSION, dataplaneVersion ?: MetricTags.UNKNOWN),
        MetricAttribute(MetricTags.STATUS_TAG, status.name),
      )
    }
  }

  /**
   * Add a metric to ease tracking launch success rate from the control plane.
   */
  private fun emitWorkloadLaunchMetric(
    workload: Workload,
    status: String,
    dataplaneVersion: String?,
  ) {
    metricClient.count(
      OssMetricsRegistry.WORKLOAD_LAUNCH_STATUS,
      1L,
      MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workload.type.name),
      MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, workload.dataplaneGroup ?: MetricTags.UNKNOWN),
      MetricAttribute(MetricTags.DATA_PLANE_ID_TAG, workload.dataplaneId ?: MetricTags.UNKNOWN),
      MetricAttribute(MetricTags.DATA_PLANE_VERSION, dataplaneVersion ?: MetricTags.UNKNOWN),
      MetricAttribute(MetricTags.STATUS_TAG, status),
    )
  }

  private fun Workload.timeSinceCreateInMillis(): Long? =
    createdAt?.let { createdAt ->
      updatedAt?.let { updatedAt ->
        updatedAt.toInstant().toEpochMilli() - createdAt.toInstant().toEpochMilli()
      }
    }

  companion object {
    val ACTIVE_STATUSES: List<WorkloadStatus> =
      listOf(WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING)
  }
}
