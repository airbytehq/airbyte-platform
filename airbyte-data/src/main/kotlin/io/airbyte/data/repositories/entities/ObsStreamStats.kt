/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.Embeddable
import io.micronaut.data.annotation.EmbeddedId
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.MappedProperty

@Embeddable
data class ObsStreamStatsId(
  @MappedProperty("job_id")
  var jobId: Long,
  @Nullable
  @MappedProperty("stream_namespace")
  var streamNamespace: String?,
  @MappedProperty("stream_name")
  var streamName: String,
)

@MappedEntity("observability_stream_stats")
data class ObsStreamStats(
  @EmbeddedId
  var id: ObsStreamStatsId,
  var bytesLoaded: Long,
  var recordsLoaded: Long,
  var recordsRejected: Long,
  var wasBackfilled: Boolean,
  var wasResumed: Boolean,
)
