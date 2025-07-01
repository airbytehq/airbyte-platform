/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.Relation
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.EnumSet

val REPLICATION_TYPES: Set<JobConfigType> = EnumSet.of(JobConfigType.sync, JobConfigType.reset_connection, JobConfigType.refresh)
val SYNC_REPLICATION_TYPES: Set<JobConfigType> = EnumSet.of(JobConfigType.sync, JobConfigType.refresh)

val TERMINAL_STATUSES =
  setOf(
    JobStatus.failed,
    JobStatus.succeeded,
    JobStatus.cancelled,
  )

val NON_CANCELLED_STATUSES = JobStatus.entries.toSet().minus(JobStatus.cancelled)

val NON_TERMINAL_STATUSES = JobStatus.entries.toSet().minus(TERMINAL_STATUSES)

@MappedEntity("jobs")
open class Job(
  @field:Id
  @AutoPopulated
  var id: Long,
  @field:TypeDef(type = DataType.OBJECT)
  var configType: JobConfigType,
  var scope: String,
  @field:TypeDef(type = DataType.JSON)
  var config: JsonNode,
  @field:TypeDef(type = DataType.OBJECT)
  var status: JobStatus,
  var startedAt: java.time.OffsetDateTime? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime,
  var isScheduled: Boolean,
)

@MappedEntity("jobs")
class JobWithAttempts(
  id: Long,
  configType: JobConfigType,
  scope: String,
  config: JsonNode,
  status: JobStatus,
  startedAt: java.time.OffsetDateTime? = null,
  createdAt: java.time.OffsetDateTime,
  updatedAt: java.time.OffsetDateTime,
  isScheduled: Boolean,
  @Relation(
    value = Relation.Kind.ONE_TO_MANY,
    mappedBy = "job",
  )
  var attempts: List<AttemptWithAssociations>? = null,
) : Job(
    id = id,
    configType = configType,
    scope = scope,
    config = config,
    status = status,
    startedAt = startedAt,
    createdAt = createdAt,
    updatedAt = updatedAt,
    isScheduled = isScheduled,
  )
