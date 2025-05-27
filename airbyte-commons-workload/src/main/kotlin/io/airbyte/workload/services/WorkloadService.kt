/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.services

import io.airbyte.commons.enums.convertTo
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.FeatureFlagClient
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

    // Create the workload and then check for mutexKey uniqueness.
    // This will lead to a more deterministic concurrency resolution in the event of concurrent create calls.
    val savedWorkload =
      workloadRepository.save(
        Workload(
          id = workloadId,
          dataplaneId = null,
          status = WorkloadStatus.PENDING,
          workloadLabels = labels,
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
            failWorkload(it.id, source = "workload-api", reason = "Superseded by $workloadId")
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
            else -> logger.error { "Cancelling workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    val workload = workloadRepository.fail(workloadId, reason = reason, source = source)
    if (workload != null) {
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
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
            else -> logger.error { "Failed workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun launchWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload = workloadRepository.launch(workloadId, deadline)
    if (workload == null) {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Trying to set a workload in a terminal state (${w.status}) to launched",
            )
            WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
              "Can't set a workload status to running on a workload that hasn't been claimed",
            )
            else -> logger.error { "Failed workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun heartbeatWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload = workloadRepository.heartbeat(workloadId, deadline)
    if (workload == null) {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Heartbeat a workload in a terminal state (${w.status})",
            )
            WorkloadStatus.PENDING -> throw InvalidStatusTransitionException("Heartbeat a non claimed workload")
            else -> logger.error { "Failed workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun runningWorkload(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    val workload = workloadRepository.running(workloadId, deadline)
    if (workload == null) {
      workloadRepository
        .findById(workloadId)
        .map { w ->
          when (w.status) {
            WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS -> throw InvalidStatusTransitionException(
              "Trying to set a workload in a terminal state (${w.status}) to running",
            )
            WorkloadStatus.PENDING -> throw InvalidStatusTransitionException(
              "Can't set a workload status to running on a workload that hasn't been claimed",
            )
            else -> logger.error { "Failed workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun succeedWorkload(workloadId: String) {
    val workload = workloadRepository.succeed(workloadId)
    if (workload != null) {
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
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

            else -> logger.error { "Failed workload $workloadId failed to update its status, status is ${w.status}" }
          }
        }.orElseThrow { NotFoundException("Workload $workloadId not found") }
    }
  }

  fun getWorkload(workloadId: String): Workload =
    workloadRepository.findById(workloadId).orElseThrow {
      NotFoundException("Could not find workload with id: $workloadId")
    }

  companion object {
    val ACTIVE_STATUSES: List<WorkloadStatus> =
      listOf(WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING)
  }
}
