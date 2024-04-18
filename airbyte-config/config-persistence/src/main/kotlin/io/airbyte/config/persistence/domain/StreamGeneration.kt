package io.airbyte.config.persistence.domain

import io.micronaut.core.annotation.NonNull
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import jakarta.persistence.Column
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("stream_generation")
data class StreamGeneration(
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
  @Column(name = "generation_id")
  @NonNull
  var generationId: Long,
  @Column(name = "start_job_id")
  @NonNull
  var startJobId: Long,
  @Column(name = "created_at")
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @Column(name = "updated_at")
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)

@MappedEntity
data class Generation(
  @Column(name = "stream_name")
  @NonNull
  val streamName: String,
  @Column(name = "stream_namespace")
  @Nullable
  val streamNamespace: String? = null,
  @Column(name = "generation_id")
  val generationId: Long,
)
