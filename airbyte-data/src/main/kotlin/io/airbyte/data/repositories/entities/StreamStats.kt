package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("stream_stats")
open class StreamStats(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var attemptId: Long,
  var streamName: String,
  var streamNamespace: String? = null,
  var recordsEmitted: Long? = null,
  var bytesEmitted: Long? = null,
  var estimatedRecords: Long? = null,
  var estimatedBytes: Long? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var bytesCommitted: Long? = null,
  var recordsCommitted: Long? = null,
  var connectionId: UUID? = null,
)
