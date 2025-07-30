/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.JobConfig
import io.airbyte.data.repositories.entities.Job
import io.airbyte.data.repositories.entities.JobWithAttempts
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityJob = Job
typealias EntityJobWithAssociations = JobWithAttempts
typealias ModelJob = io.airbyte.config.Job

typealias EntityConfigType = JobConfigType
typealias ModelConfigType = JobConfig.ConfigType

typealias EntityJobStatus = JobStatus
typealias ModelJobStatus = io.airbyte.config.JobStatus

fun ModelJobStatus.toEntity(): EntityJobStatus =
  when (this) {
    ModelJobStatus.PENDING -> EntityJobStatus.pending
    ModelJobStatus.RUNNING -> EntityJobStatus.running
    ModelJobStatus.SUCCEEDED -> EntityJobStatus.succeeded
    ModelJobStatus.FAILED -> EntityJobStatus.failed
    ModelJobStatus.CANCELLED -> EntityJobStatus.cancelled
    ModelJobStatus.INCOMPLETE -> EntityJobStatus.incomplete
  }

fun EntityJobStatus.toConfig(): ModelJobStatus =
  when (this) {
    EntityJobStatus.pending -> ModelJobStatus.PENDING
    EntityJobStatus.running -> ModelJobStatus.RUNNING
    EntityJobStatus.succeeded -> ModelJobStatus.SUCCEEDED
    EntityJobStatus.failed -> ModelJobStatus.FAILED
    EntityJobStatus.cancelled -> ModelJobStatus.CANCELLED
    EntityJobStatus.incomplete -> ModelJobStatus.INCOMPLETE
  }

fun EntityJobWithAssociations.toConfigModel(): ModelJob =
  ModelJob(
    this.id,
    this.configType.toConfig(),
    this.scope,
    Jsons.`object`(this.config, JobConfig::class.java),
    this.attempts?.map { it.toConfigModel() } ?: emptyList(),
    this.status.toConfig(),
    startedAt?.toEpochSecond(),
    createdAt.toEpochSecond(),
    updatedAt.toEpochSecond(),
    this.isScheduled,
  )

fun EntityJob.toConfigModel(): ModelJob =
  ModelJob(
    this.id,
    this.configType.toConfig(),
    this.scope,
    Jsons.`object`(this.config, JobConfig::class.java),
    emptyList(),
    this.status.toConfig(),
    startedAt?.toEpochSecond(),
    createdAt.toEpochSecond(),
    updatedAt.toEpochSecond(),
    this.isScheduled,
  )

fun ModelJob.toEntity(): EntityJob =
  EntityJob(
    id,
    this.configType.convertTo<JobConfigType>(),
    this.scope,
    Jsons.jsonNode(config),
    this.status.convertTo<JobStatus>(),
    startedAtInSecond?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    OffsetDateTime.ofInstant(Instant.ofEpochSecond(createdAtInSecond), ZoneOffset.UTC),
    OffsetDateTime.ofInstant(Instant.ofEpochSecond(updatedAtInSecond), ZoneOffset.UTC),
    false,
  )

fun EntityConfigType.toConfig(): ModelConfigType =
  when (this) {
    EntityConfigType.sync -> ModelConfigType.SYNC
    EntityConfigType.reset_connection -> ModelConfigType.RESET_CONNECTION
    EntityConfigType.refresh -> ModelConfigType.REFRESH
    EntityConfigType.check_connection_source -> ModelConfigType.CHECK_CONNECTION_SOURCE
    EntityConfigType.check_connection_destination -> ModelConfigType.CHECK_CONNECTION_DESTINATION
    EntityConfigType.discover_schema -> ModelConfigType.DISCOVER_SCHEMA
    EntityConfigType.get_spec -> ModelConfigType.GET_SPEC
  }

fun ModelConfigType.toEntity(): EntityConfigType =
  when (this) {
    ModelConfigType.SYNC -> EntityConfigType.sync
    ModelConfigType.RESET_CONNECTION -> EntityConfigType.reset_connection
    ModelConfigType.REFRESH -> EntityConfigType.refresh
    ModelConfigType.CHECK_CONNECTION_SOURCE -> EntityConfigType.check_connection_source
    ModelConfigType.CHECK_CONNECTION_DESTINATION -> EntityConfigType.check_connection_destination
    ModelConfigType.DISCOVER_SCHEMA -> EntityConfigType.discover_schema
    ModelConfigType.GET_SPEC -> EntityConfigType.get_spec
    JobConfig.ConfigType.CLEAR -> EntityConfigType.reset_connection
  }
