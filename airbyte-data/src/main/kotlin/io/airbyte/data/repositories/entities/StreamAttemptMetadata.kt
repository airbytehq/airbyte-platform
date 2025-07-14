/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("stream_attempt_metadata")
data class StreamAttemptMetadata(
  @field:Id
  @AutoPopulated
  val id: UUID? = null,
  val attemptId: Long,
  val streamNamespace: String?,
  val streamName: String,
  val wasBackfilled: Boolean?,
  val wasResumed: Boolean?,
)
