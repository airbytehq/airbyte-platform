/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
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
import io.airbyte.workload.signal.SignalSender
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID
import io.airbyte.workload.repository.domain.Workload as DomainWorkload

private val logger = KotlinLogging.logger {}

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
class WorkloadHandlerImpl(
  private val workloadService: WorkloadService,
  private val workloadRepository: WorkloadRepository,
  private val workloadQueueRepository: WorkloadQueueRepository,
  private val signalSender: SignalSender,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
  @Property(name = "airbyte.workload-api.workload-redelivery-window") private val workloadRedeliveryWindow: Duration,
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
  ): Boolean {
    val workload = workloadRepository.claim(workloadId, dataplaneId, deadline)
    val claimed = workload != null && workload.status == WorkloadStatus.CLAIMED && workload.dataplaneId == dataplaneId
    if (claimed) {
      workload?.let { emitTimeToTransitionMetric(it, WorkloadStatus.CLAIMED) }
      workloadQueueRepository.ackWorkloadQueueItem(workloadId)
    }
    return claimed
  }

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
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.failWorkload(workloadId, source, reason)
    }
  }

  override fun succeedWorkload(workloadId: String) {
    withWorkloadServiceExceptionConverter {
      workloadService.succeedWorkload(workloadId)
    }
  }

  override fun setWorkloadStatusToRunning(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.runningWorkload(workloadId, deadline)
    }
  }

  override fun setWorkloadStatusToLaunched(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.launchWorkload(workloadId, deadline)
    }
  }

  override fun heartbeat(
    workloadId: String,
    deadline: OffsetDateTime,
  ) {
    withWorkloadServiceExceptionConverter {
      workloadService.heartbeatWorkload(workloadId, deadline)
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

  override fun pollWorkloadQueue(
    dataplaneGroup: String?,
    priority: WorkloadPriority?,
    quantity: Int,
  ): List<Workload> {
    val domainWorkloads =
      workloadQueueRepository.pollWorkloadQueue(
        dataplaneGroup,
        priority?.toInt(),
        quantity,
        redeliveryWindowSecs = workloadRedeliveryWindow.seconds.toInt(),
      )

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

  private fun emitTimeToTransitionMetric(
    workload: DomainWorkload,
    status: WorkloadStatus,
  ) {
    workload.timeSinceCreateInMillis()?.let {
      metricClient.distribution(
        OssMetricsRegistry.WORKLOAD_TIME_TO_TRANSITION_FROM_CREATE,
        it.toDouble(),
        MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workload.type.name),
        MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, workload.dataplaneGroup ?: "undefined"),
        MetricAttribute(MetricTags.DATA_PLANE_ID_TAG, workload.dataplaneId ?: "undefined"),
        MetricAttribute(MetricTags.STATUS_TAG, status.name),
      )
    }
  }

  private fun DomainWorkload.timeSinceCreateInMillis(): Long? =
    createdAt?.let { createdAt ->
      updatedAt?.let { updatedAt ->
        updatedAt.toInstant().toEpochMilli() - createdAt.toInstant().toEpochMilli()
      }
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
