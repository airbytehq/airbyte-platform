package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

@MappedEntity("attempts")
open class Attempt(
  @field:Id
  @AutoPopulated
  var id: Long? = null,
  var jobId: Long? = null,
  var attemptNumber: Long? = null,
  var logPath: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var output: JsonNode? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var status: AttemptStatus? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var endedAt: java.time.OffsetDateTime? = null,
  var temporalWorkflowId: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var failureSummary: JsonNode? = null,
  var processingTaskQueue: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var attemptSyncConfig: JsonNode? = null,
)
