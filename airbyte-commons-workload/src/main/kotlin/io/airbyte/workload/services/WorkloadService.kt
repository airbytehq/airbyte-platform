/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.services

import io.airbyte.config.WorkloadPriority
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
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
) {
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
          logPath = logPath,
          mutexKey = mutexKey,
          type = type,
          autoId = autoId,
          deadline = deadline,
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
    val workload = getWorkload(workloadId)

    when (workload.status) {
      WorkloadStatus.PENDING, WorkloadStatus.LAUNCHED, WorkloadStatus.CLAIMED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.CANCELLED,
          source,
          reason,
          null,
        )
        signalSender.sendSignal(workload.type, workload.signalInput)

        workloadQueueRepository.ackWorkloadQueueItem(workloadId)
      }
      WorkloadStatus.CANCELLED -> logger.info { "Workload $workloadId is already cancelled. Cancelling an already cancelled workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Cannot cancel a workload in either success or failure status. Workload id: $workloadId has status: ${workload.status}",
      )
    }
  }

  fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    val workload = getWorkload(workloadId)
    when (workload.status) {
      WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING -> {
        workloadRepository.update(
          workloadId,
          WorkloadStatus.FAILURE,
          source,
          reason,
          null,
        )
        signalSender.sendSignal(workload.type, workload.signalInput)

        workloadQueueRepository.ackWorkloadQueueItem(workloadId)
      }

      WorkloadStatus.FAILURE -> logger.info { "Workload $workloadId is already marked as failed. Failing an already failed workload is a noop" }
      else -> throw InvalidStatusTransitionException(
        "Tried to fail a workload that is not active. Workload id: $workloadId has status: ${workload.status}",
      )
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
