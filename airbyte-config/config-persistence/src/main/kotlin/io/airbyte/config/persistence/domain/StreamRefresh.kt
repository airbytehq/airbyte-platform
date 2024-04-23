package io.airbyte.config.persistence.domain

import io.micronaut.core.annotation.NonNull
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import jakarta.persistence.Column
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("stream_refreshes")
data class StreamRefresh(
  @field:Id
  @NonNull
  var id: UUID? = UUID.randomUUID(),
  @Column(name = "connection_id")
  @NonNull
  var connectionId: UUID,
  @Column(name = "stream_name")
  @NonNull
  var streamName: String,
  @Column(name = "stream_namespace")
  @Nullable
  var streamNamespace: String? = null,
  @Column(name = "created_at")
  @DateCreated
  var createdAt: OffsetDateTime? = null,
)
