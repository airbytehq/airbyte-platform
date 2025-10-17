/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.handler

import io.airbyte.config.WorkloadPriority
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadSummaryDTO
import io.airbyte.workload.repository.domain.WorkloadType
import java.util.UUID

typealias ApiWorkloadStatus = io.airbyte.workload.api.domain.WorkloadStatus
typealias DomainWorkload = Workload
typealias ApiWorkload = io.airbyte.workload.api.domain.Workload
typealias DomainWorkloadDTO = WorkloadSummaryDTO
typealias ApiWorkloadSummary = io.airbyte.workload.api.domain.WorkloadSummary
typealias DomainWorkloadLabel = WorkloadLabel
typealias ApiWorkloadLabel = io.airbyte.workload.api.domain.WorkloadLabel
typealias ApiWorkloadType = io.airbyte.config.WorkloadType
typealias ApiWorkloadQueueStats = io.airbyte.workload.api.domain.WorkloadQueueStats
typealias DomainWorkloadQueueStats = io.airbyte.workload.repository.domain.WorkloadQueueStats

fun ApiWorkloadStatus.toDomain(): WorkloadStatus =
  when (this) {
    ApiWorkloadStatus.PENDING -> WorkloadStatus.PENDING
    ApiWorkloadStatus.CLAIMED -> WorkloadStatus.CLAIMED
    ApiWorkloadStatus.LAUNCHED -> WorkloadStatus.LAUNCHED
    ApiWorkloadStatus.RUNNING -> WorkloadStatus.RUNNING
    ApiWorkloadStatus.SUCCESS -> WorkloadStatus.SUCCESS
    ApiWorkloadStatus.FAILURE -> WorkloadStatus.FAILURE
    ApiWorkloadStatus.CANCELLED -> WorkloadStatus.CANCELLED
  }

fun WorkloadStatus.toApi(): ApiWorkloadStatus =
  when (this) {
    WorkloadStatus.PENDING -> ApiWorkloadStatus.PENDING
    WorkloadStatus.CLAIMED -> ApiWorkloadStatus.CLAIMED
    WorkloadStatus.LAUNCHED -> ApiWorkloadStatus.LAUNCHED
    WorkloadStatus.RUNNING -> ApiWorkloadStatus.RUNNING
    WorkloadStatus.SUCCESS -> ApiWorkloadStatus.SUCCESS
    WorkloadStatus.FAILURE -> ApiWorkloadStatus.FAILURE
    WorkloadStatus.CANCELLED -> ApiWorkloadStatus.CANCELLED
  }

fun ApiWorkloadType.toDomain(): WorkloadType =
  when (this) {
    ApiWorkloadType.CHECK -> WorkloadType.CHECK
    ApiWorkloadType.DISCOVER -> WorkloadType.DISCOVER
    ApiWorkloadType.SPEC -> WorkloadType.SPEC
    ApiWorkloadType.SYNC -> WorkloadType.SYNC
  }

fun WorkloadType.toApi(): ApiWorkloadType =
  when (this) {
    WorkloadType.CHECK -> ApiWorkloadType.CHECK
    WorkloadType.DISCOVER -> ApiWorkloadType.DISCOVER
    WorkloadType.SPEC -> ApiWorkloadType.SPEC
    WorkloadType.SYNC -> ApiWorkloadType.SYNC
  }

fun DomainWorkload.toApi(): ApiWorkload =
  ApiWorkload(
    id = this.id,
    dataplaneId = this.dataplaneId,
    status = this.status.toApi(),
    labels = this.getLabels(),
    inputPayload = this.inputPayload,
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
    logPath = this.logPath,
    mutexKey = this.mutexKey,
    type = this.type.toApi(),
    terminationReason = this.terminationReason,
    terminationSource = this.terminationSource,
    autoId = if (this.autoId == null) UUID(0, 0) else this.autoId!!,
    signalInput = this.signalInput,
    dataplaneGroup = this.dataplaneGroup,
    priority = this.priority?.let { WorkloadPriority.fromInt(it) },
  )

/**
 * Get labels with fallback logic: prefer labels JSONB column, fallback to workloadLabels table.
 * This supports the dual-write migration strategy where new workloads have labels in JSONB,
 * but old workloads still use the workload_label table.
 */
private fun DomainWorkload.getLabels(): MutableList<ApiWorkloadLabel> {
  // If labels JSONB field is present, use it (new approach)
  this.labels?.let { labelsMap ->
    if (labelsMap.isNotEmpty()) {
      return labelsMap
        .map { (key, value) -> ApiWorkloadLabel(key = key, value = value) }
        .toMutableList()
    }
  }

  // Otherwise, fallback to workloadLabels table (legacy approach)
  return this.workloadLabels
    ?.map { it.toApi() }
    ?.toMutableList()
    ?: mutableListOf()
}

fun DomainWorkloadDTO.toApi(): ApiWorkloadSummary =
  ApiWorkloadSummary(
    id = id,
    autoId = autoId.toString(),
    status = status.toApi(),
    deadline = deadline,
  )

fun DomainWorkloadLabel.toApi(): ApiWorkloadLabel =
  ApiWorkloadLabel(
    key = this.key,
    value = this.value,
  )

fun ApiWorkloadLabel.toDomain(): DomainWorkloadLabel =
  DomainWorkloadLabel(
    key = this.key,
    value = this.value,
  )

fun DomainWorkloadQueueStats.toApi(): ApiWorkloadQueueStats =
  ApiWorkloadQueueStats(
    dataplaneGroup = dataplaneGroup,
    priority = priority?.let { WorkloadPriority.fromInt(it) },
    enqueuedCount = enqueuedCount,
  )
