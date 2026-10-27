/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("stream_statuses")
data class StreamStatusAuthorization(
  @field:Id val id: UUID,
  val workspaceId: UUID,
  val connectionId: UUID,
  val jobId: Long,
  val attemptNumber: Int,
  val jobType: String,
)
