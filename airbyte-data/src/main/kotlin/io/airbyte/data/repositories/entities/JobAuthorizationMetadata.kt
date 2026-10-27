/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType

@MappedEntity("jobs")
data class JobAuthorizationMetadata(
  @field:Id val id: Long,
  @field:TypeDef(type = DataType.OBJECT) val configType: JobConfigType,
  val scope: String,
)

@MappedEntity("attempts")
data class AttemptAuthorizationMetadata(
  @field:Id val id: Long?,
  val jobId: Long?,
  val attemptNumber: Long?,
)
