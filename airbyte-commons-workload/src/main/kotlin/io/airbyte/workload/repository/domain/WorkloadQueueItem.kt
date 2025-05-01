/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository.domain

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("workload_queue")
data class WorkloadQueueItem(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var dataplaneGroup: String?,
  var priority: Int,
  var workloadId: String,
  var pollDeadline: OffsetDateTime?,
  @Nullable
  var ackedAt: OffsetDateTime? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
