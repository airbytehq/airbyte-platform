/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseDeadlineInWorkloadMonitorQueries
import io.airbyte.featureflag.UseWorkloadLabelsJsonbOnly
import io.airbyte.micronaut.runtime.AirbyteWorkloadApiClientConfig
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadLabel
import io.airbyte.workload.api.domain.WorkloadQueueStats
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.WorkloadRepository
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.WorkloadService
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID
import io.airbyte.workload.repository.domain.Workload as DomainWorkload

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
class WorkloadHandlerImpl(
  private val workloadService: WorkloadService,
  private val workloadRepository: WorkloadRepository,
  private val workloadQueueRepository: WorkloadQueueRepository,
  private val airbyteWorkloadApiClientConfig: AirbyteWorkloadApiClientConfig,
  private val featureFlagClient: FeatureFlagClient,
) : WorkloadHandler {
  override fun getWorkload(workloadId: String): ApiWorkload = getDomainWorkload(workloadId).toApi()

  private fun getDomainWorkload(workloadId: String): DomainWorkload =
    withWorkloadServiceExceptionConverter {
      workloadService.getWorkload(workloadId)
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

  override fun workloadAlreadyExists(workloadId: String): Boolean = workloadRepository.existsById(workloadId)

  override fun createWorkload(
    workloadId: String,
    labels: List<WorkloadLabel>?,
    input: String,
    workspaceId: UUID?,
    organizationId: UUID?,
    logPath: String,
    mutexKey: String?,
    type: WorkloadType,
    autoId: UUID,
    deadline: OffsetDateTime,
    signalInput: String?,
    dataplaneGroup: String?,
    priority: WorkloadPriority?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.createWorkload(
        workloadId = workloadId,
        labels = labels?.map { it.toDomain() },
        logPath = logPath,
        input = input,
        workspaceId = workspaceId,
        organizationId = organizationId,
        mutexKey = mutexKey,
        type = type,
        autoId = autoId,
        deadline = deadline,
        signalInput = signalInput,
        dataplaneGroup = dataplaneGroup,
        priority = priority,
      )
    }
  }

  override fun claimWorkload(
    workloadId: String,
    dataplaneId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ): Boolean = workloadService.claimWorkload(workloadId, dataplaneId, deadline, dataplaneVersion) != null

  override fun cancelWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.cancelWorkload(workloadId, source, reason)
    }
  }

  override fun failWorkload(
    workloadId: String,
    source: String?,
    reason: String?,
    dataplaneVersion: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.failWorkload(workloadId, source, reason, dataplaneVersion)
    }
  }

  override fun succeedWorkload(
    workloadId: String,
    dataplaneVersion: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.succeedWorkload(workloadId, dataplaneVersion)
    }
  }

  override fun setWorkloadStatusToRunning(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.runningWorkload(workloadId, deadline, dataplaneVersion)
    }
  }

  override fun setWorkloadStatusToLaunched(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.launchWorkload(workloadId, deadline, dataplaneVersion)
    }
  }

  override fun heartbeat(
    workloadId: String,
    deadline: OffsetDateTime,
    dataplaneVersion: String?,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.heartbeatWorkload(workloadId, deadline, dataplaneVersion)
    }
  }

  fun offsetDateTime(): OffsetDateTime = OffsetDateTime.now()

  override fun getWorkloadsRunningCreatedBefore(
    dataplaneId: List<String>?,
    workloadType: List<ApiWorkloadType>?,
    createdBefore: OffsetDateTime?,
  ): List<Workload> {
    val useDeadline = featureFlagClient.boolVariation(UseDeadlineInWorkloadMonitorQueries, Empty)
    val domainWorkloads =
      if (useDeadline) {
        workloadRepository.searchByTypeStatusAndCreationDateWithDeadline(
          dataplaneId,
          listOf(WorkloadStatus.RUNNING),
          workloadType?.map { it.toDomain() },
          createdBefore,
        )
      } else {
        workloadRepository.searchByTypeStatusAndCreationDate(
          dataplaneId,
          listOf(WorkloadStatus.RUNNING),
          workloadType?.map { it.toDomain() },
          createdBefore,
        )
      }

    return domainWorkloads.map { it.toApi() }
  }

  override fun pollWorkloadQueue(
    dataplaneGroup: String?,
    priority: WorkloadPriority?,
    quantity: Int,
  ): List<Workload> {
    val useJsonbLabelsOnly = featureFlagClient.boolVariation(UseWorkloadLabelsJsonbOnly, Empty)
    val domainWorkloads =
      if (useJsonbLabelsOnly) {
        workloadQueueRepository.pollWorkloadQueueWithoutLegacyLabels(
          dataplaneGroup,
          priority?.toInt(),
          quantity,
          redeliveryWindowSecs = airbyteWorkloadApiClientConfig.workloadRedeliveryWindowSeconds,
        )
      } else {
        workloadQueueRepository.pollWorkloadQueue(
          dataplaneGroup,
          priority?.toInt(),
          quantity,
          redeliveryWindowSecs = airbyteWorkloadApiClientConfig.workloadRedeliveryWindowSeconds,
        )
      }

    return domainWorkloads.map { it.toApi() }
  }

  override fun countWorkloadQueueDepth(
    dataplaneGroup: String?,
    priority: WorkloadPriority?,
  ): Long = workloadQueueRepository.countEnqueuedWorkloads(dataplaneGroup, priority?.toInt())

  override fun getWorkloadQueueStats(): List<WorkloadQueueStats> {
    val domainStats =
      workloadQueueRepository.getEnqueuedWorkloadStats()

    return domainStats.map { it.toApi() }
  }

  override fun cleanWorkloadQueue(limit: Int) {
    workloadQueueRepository.cleanUpAckedEntries(limit)
  }

  override fun getActiveWorkloads(
    dataplaneIds: List<String>?,
    statuses: List<ApiWorkloadStatus>?,
  ): List<ApiWorkloadSummary> {
    val domainWorkloadsDTO =
      workloadRepository.searchActive(
        dataplaneIds = dataplaneIds,
        statuses = statuses?.map { it.toDomain() },
      )
    return domainWorkloadsDTO.map { it.toApi() }
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

  private fun <T> withWorkloadServiceExceptionConverter(f: () -> T): T {
    try {
      return f()
    } catch (e: io.airbyte.workload.services.ConflictException) {
      throw ConflictException(e.message)
    } catch (e: io.airbyte.workload.services.InvalidStatusTransitionException) {
      throw InvalidStatusTransitionException(e.message)
    } catch (e: io.airbyte.workload.services.NotFoundException) {
      throw NotFoundException(e.message)
    }
  }
}
