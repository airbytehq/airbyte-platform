package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

@MappedEntity("jobs")
open class Job(
  @field:Id
  @AutoPopulated
  var id: Long? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var configType: JobConfigType? = null,
  var scope: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var config: JsonNode? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var status: JobStatus? = null,
  var startedAt: java.time.OffsetDateTime? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
