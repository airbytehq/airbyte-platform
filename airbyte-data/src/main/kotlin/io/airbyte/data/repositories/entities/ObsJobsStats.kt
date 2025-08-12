/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("observability_jobs_stats")
data class ObsJobsStats(
  @field:Id
  var jobId: Long,
  var connectionId: UUID,
  var workspaceId: UUID,
  var organizationId: UUID,
  var sourceId: UUID,
  var sourceDefinitionId: UUID,
  var sourceImageTag: String,
  var destinationId: UUID,
  var destinationDefinitionId: UUID,
  var destinationImageTag: String,
  var createdAt: OffsetDateTime,
  var jobType: String,
  var status: String,
  var attemptCount: Int,
  var durationSeconds: Long,
)
