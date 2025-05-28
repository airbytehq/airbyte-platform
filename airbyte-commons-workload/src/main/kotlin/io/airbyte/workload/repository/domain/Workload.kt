/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository.domain

import com.google.common.annotations.VisibleForTesting
import io.micronaut.context.annotation.Factory
import io.micronaut.core.annotation.Nullable
import io.micronaut.core.convert.ConversionContext
import io.micronaut.core.convert.TypeConverter
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.Relation
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

@MappedEntity("workload")
data class Workload(
  @field:Id
  var id: String,
  @Nullable
  var dataplaneId: String?,
  @field:TypeDef(type = DataType.OBJECT)
  var status: WorkloadStatus,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
  @Nullable
  var lastHeartbeatAt: OffsetDateTime? = null,
  @Relation(
    value = Relation.Kind.ONE_TO_MANY,
    mappedBy = "workload",
    cascade = [Relation.Cascade.ALL],
  )
  @Nullable
  var workloadLabels: List<WorkloadLabel>?,
  var inputPayload: String,
  @Nullable
  var workspaceId: UUID?,
  @Nullable
  var organizationId: UUID?,
  var logPath: String,
  @Nullable
  var mutexKey: String?,
  @field:TypeDef(type = DataType.OBJECT)
  var type: WorkloadType,
  @Nullable
  var terminationSource: String? = null,
  @Nullable
  var terminationReason: String? = null,
  @Nullable
  var deadline: OffsetDateTime? = null,
  @Nullable
  var autoId: UUID? = null,
  @Nullable
  var signalInput: String? = null,
  @Nullable
  var dataplaneGroup: String? = null,
  @Nullable
  var priority: Int? = null,
) {
  @VisibleForTesting
  constructor(
    id: String,
    dataplaneId: String?,
    status: WorkloadStatus,
    workloadLabels: List<WorkloadLabel>?,
    inputPayload: String,
    workspaceId: UUID?,
    organizationId: UUID?,
    logPath: String,
    mutexKey: String,
    type: WorkloadType,
    signalInput: String,
  ) : this(
    id = id,
    dataplaneId = dataplaneId,
    status = status,
    workloadLabels = workloadLabels,
    inputPayload = inputPayload,
    workspaceId = workspaceId,
    organizationId = organizationId,
    logPath = logPath,
    mutexKey = mutexKey,
    type = type,
    lastHeartbeatAt = null,
    createdAt = null,
    updatedAt = null,
    terminationSource = null,
    terminationReason = null,
    autoId = UUID.randomUUID(),
    signalInput = signalInput,
  )
}

@TypeDef(type = DataType.STRING)
enum class WorkloadStatus {
  PENDING,
  CLAIMED,
  LAUNCHED,
  RUNNING,
  SUCCESS,
  FAILURE,
  CANCELLED,
  ;

  override fun toString(): String = name.lowercase()
}

@Factory
class WorkloadStatusTypeConverters {
  @Singleton
  fun workloadStatusToStringTypeConverter(): TypeConverter<WorkloadStatus, String> =
    TypeConverter {
      workloadStatus,
      _: Class<String>,
      _: ConversionContext,
      ->
      Optional.of(workloadStatus.toString())
    }
}

@TypeDef(type = DataType.STRING)
enum class WorkloadType {
  SYNC,
  CHECK,
  DISCOVER,
  SPEC,
  ;

  override fun toString(): String = name.lowercase()

  @Factory
  class WorkloadTypeTypeConverters {
    @Singleton
    fun workloadTypeToStringTypeConverter(): TypeConverter<WorkloadType, String> =
      TypeConverter {
        workloadType,
        _: Class<String>,
        _: ConversionContext,
        ->
        Optional.of(workloadType.toString())
      }
  }
}
