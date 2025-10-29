/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.Embeddable
import io.micronaut.data.annotation.EmbeddedId
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.MappedProperty
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.math.BigDecimal

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
  @field:TypeDef(type = DataType.JSON)
  var additionalStats: Map<String, BigDecimal>? = null,
)
