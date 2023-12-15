package io.airbyte.workload.handler

import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType

typealias ApiWorkloadStatus = io.airbyte.workload.api.domain.WorkloadStatus
typealias DomainWorkload = Workload
typealias ApiWorkload = io.airbyte.workload.api.domain.Workload
typealias DomainWorkloadLabel = WorkloadLabel
typealias ApiWorkloadLabel = io.airbyte.workload.api.domain.WorkloadLabel
typealias ApiWorkloadType = io.airbyte.config.WorkloadType

fun ApiWorkloadStatus.toDomain(): WorkloadStatus {
  return when (this) {
    ApiWorkloadStatus.PENDING -> WorkloadStatus.PENDING
    ApiWorkloadStatus.CLAIMED -> WorkloadStatus.CLAIMED
    ApiWorkloadStatus.LAUNCHED -> WorkloadStatus.LAUNCHED
    ApiWorkloadStatus.RUNNING -> WorkloadStatus.RUNNING
    ApiWorkloadStatus.SUCCESS -> WorkloadStatus.SUCCESS
    ApiWorkloadStatus.FAILURE -> WorkloadStatus.FAILURE
    ApiWorkloadStatus.CANCELLED -> WorkloadStatus.CANCELLED
  }
}

fun WorkloadStatus.toApi(): ApiWorkloadStatus {
  return when (this) {
    WorkloadStatus.PENDING -> ApiWorkloadStatus.PENDING
    WorkloadStatus.CLAIMED -> ApiWorkloadStatus.CLAIMED
    WorkloadStatus.LAUNCHED -> ApiWorkloadStatus.LAUNCHED
    WorkloadStatus.RUNNING -> ApiWorkloadStatus.RUNNING
    WorkloadStatus.SUCCESS -> ApiWorkloadStatus.SUCCESS
    WorkloadStatus.FAILURE -> ApiWorkloadStatus.FAILURE
    WorkloadStatus.CANCELLED -> ApiWorkloadStatus.CANCELLED
  }
}

fun ApiWorkloadType.toDomain(): WorkloadType {
  return when (this) {
    ApiWorkloadType.CHECK -> WorkloadType.CHECK
    ApiWorkloadType.DISCOVER -> WorkloadType.DISCOVER
    ApiWorkloadType.SPEC -> WorkloadType.SPEC
    ApiWorkloadType.SYNC -> WorkloadType.SYNC
  }
}

fun WorkloadType.toApi(): ApiWorkloadType {
  return when (this) {
    WorkloadType.CHECK -> ApiWorkloadType.CHECK
    WorkloadType.DISCOVER -> ApiWorkloadType.DISCOVER
    WorkloadType.SPEC -> ApiWorkloadType.SPEC
    WorkloadType.SYNC -> ApiWorkloadType.SYNC
  }
}

fun DomainWorkload.toApi(): ApiWorkload {
  return ApiWorkload(
    id = this.id,
    dataplaneId = this.dataplaneId,
    status = this.status.toApi(),
    labels = this.workloadLabels?.map { it.toApi() }?.toMutableList() ?: mutableListOf(),
    inputPayload = this.inputPayload,
    logPath = this.logPath,
    geography = this.geography,
    mutexKey = this.mutexKey,
    type = this.type.toApi(),
  )
}

fun ApiWorkload.toDomain(): DomainWorkload {
  return DomainWorkload(
    id = this.id,
    dataplaneId = this.dataplaneId,
    status = this.status?.toDomain() ?: WorkloadStatus.PENDING,
    workloadLabels = this.labels?.map { it.toDomain() },
    inputPayload = this.inputPayload,
    logPath = this.logPath,
    geography = this.geography,
    mutexKey = this.mutexKey,
    type = this.type.toDomain(),
    terminationReason = terminationReason,
    terminationSource = terminationSource,
  )
}

fun DomainWorkloadLabel.toApi(): ApiWorkloadLabel {
  return ApiWorkloadLabel(
    key = this.key,
    value = this.key,
  )
}

fun ApiWorkloadLabel.toDomain(): DomainWorkloadLabel {
  return DomainWorkloadLabel(
    key = this.key,
    value = this.value,
  )
}
