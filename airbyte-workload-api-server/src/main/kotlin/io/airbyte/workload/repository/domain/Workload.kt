package io.airbyte.workload.repository.domain

import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadType
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.Relation
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime

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
  var logPath: String,
  var geography: String,
  @Nullable
  var mutexKey: String?,
  @field:TypeDef(type = DataType.OBJECT)
  var type: WorkloadType,
) {
  @JvmOverloads
  constructor(
    id: String,
    dataplaneId: String?,
    status: WorkloadStatus,
    workloadLabels: List<WorkloadLabel>?,
    inputPayload: String,
    logPath: String,
    geography: String,
    mutexKey: String,
    type: WorkloadType,
  ) : this(
    id = id,
    dataplaneId = dataplaneId,
    status = status,
    workloadLabels = workloadLabels,
    inputPayload = inputPayload,
    logPath = logPath,
    geography = geography,
    mutexKey = mutexKey,
    type = type,
    lastHeartbeatAt = null,
    createdAt = null,
    updatedAt = null,
  )
}
