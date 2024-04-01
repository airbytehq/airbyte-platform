package io.airbyte.config.persistence.domain

import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Embeddable
import io.micronaut.data.annotation.EmbeddedId
import io.micronaut.data.annotation.MappedEntity
import jakarta.persistence.Column
import java.time.OffsetDateTime
import java.util.UUID

@Embeddable
data class StreamRefreshPK(
  @Column(name = "connection_id")
  val connectionId: UUID,
  @Column(name = "stream_name")
  val streamName: String,
  @Column(name = "stream_namespace")
  @Nullable
  val streamNamespace: String? = null,
)

@MappedEntity("stream_refreshes")
data class StreamRefresh(
  @EmbeddedId
  val pk: StreamRefreshPK,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
)
