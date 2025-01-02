/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

/**
 * DTO for our data access layer.
 */
@MappedEntity("stream_statuses")
class StreamStatus(
  @field:AutoPopulated @field:Id val id: UUID?,
  val workspaceId: UUID?,
  val connectionId: UUID?,
  val jobId: Long?,
  val attemptNumber: Int?,
  val streamNamespace: @Nullable String?,
  val streamName: String?,
  @field:TypeDef(type = DataType.OBJECT) val jobType: JobStreamStatusJobType?,
  @field:DateCreated val createdAt: OffsetDateTime?,
  @field:DateUpdated val updatedAt: OffsetDateTime?,
  @field:TypeDef(type = DataType.OBJECT) val runState: JobStreamStatusRunState?,
  @field:TypeDef(type = DataType.OBJECT) val incompleteRunCause: @Nullable JobStreamStatusIncompleteRunCause?,
  val transitionedAt: OffsetDateTime?,
  @field:TypeDef(type = DataType.JSON) val metadata: @Nullable JsonNode?,
) {
  override fun equals(other: Any?): Boolean {
    if (other === this) {
      return true
    }
    if (other !is StreamStatus) {
      return false
    }
    if (!other.canEqual(this)) {
      return false
    }
    val thisWorkspaceId: Any? = this.workspaceId
    val otherWorkspaceId: Any? = other.workspaceId
    if (thisWorkspaceId != otherWorkspaceId) {
      return false
    }
    val thisConnectionId: Any? = this.connectionId
    val otherConnectionId: Any? = other.connectionId
    if (thisConnectionId != otherConnectionId) {
      return false
    }
    val thisJobId: Any? = this.jobId
    val otherJobId: Any? = other.jobId
    if (thisJobId != otherJobId) {
      return false
    }
    val thisAttemptNumber: Any? = this.attemptNumber
    val otherAttemptNumber: Any? = other.attemptNumber
    if (thisAttemptNumber != otherAttemptNumber) {
      return false
    }
    val thisStreamNamespace: Any? = this.streamNamespace
    val otherStreamNamespace: Any? = other.streamNamespace
    if (thisStreamNamespace != otherStreamNamespace) {
      return false
    }
    val thisStreamName: Any? = this.streamName
    val otherStreamName: Any? = other.streamName
    if (thisStreamName != otherStreamName) {
      return false
    }
    val thisJobType: Any? = this.jobType
    val otherJobType: Any? = other.jobType
    if (thisJobType != otherJobType) {
      return false
    }
    val thisRunState: Any? = this.runState
    val otherRunState: Any? = other.runState
    if (thisRunState != otherRunState) {
      return false
    }
    val thisIncompleteRunCause: Any? = this.incompleteRunCause
    val otherIncompleteRunCause: Any? = other.incompleteRunCause
    if (thisIncompleteRunCause != otherIncompleteRunCause) {
      return false
    }
    val thisTransitionedAt: Any? = this.transitionedAt
    val otherTransitionedAt: Any? = other.transitionedAt
    if (thisTransitionedAt != otherTransitionedAt) {
      return false
    }
    val thisMetadata: JsonNode? = this.metadata
    val otherMetadata: JsonNode? = other.metadata

    return thisMetadata?.asText() == otherMetadata?.asText()
  }

  private fun canEqual(other: Any?): Boolean {
    return other is StreamStatus
  }

  override fun hashCode(): Int {
    val prime = 59
    var result = 1
    val workspaceId: Any? = this.workspaceId
    result = result * prime + (workspaceId?.hashCode() ?: 43)
    val connectionId: Any? = this.connectionId
    result = result * prime + (connectionId?.hashCode() ?: 43)
    val jobId: Any? = this.jobId
    result = result * prime + (jobId?.hashCode() ?: 43)
    val attemptNumber: Any? = this.attemptNumber
    result = result * prime + (attemptNumber?.hashCode() ?: 43)
    val streamNamespace: Any? = this.streamNamespace
    result = result * prime + (streamNamespace?.hashCode() ?: 43)
    val streamName: Any? = this.streamName
    result = result * prime + (streamName?.hashCode() ?: 43)
    val jobType: Any? = this.jobType
    result = result * prime + (jobType?.hashCode() ?: 43)
    val runState: Any? = this.runState
    result = result * prime + (runState?.hashCode() ?: 43)
    val incompleteRunCause: Any? = this.incompleteRunCause
    result = result * prime + (incompleteRunCause?.hashCode() ?: 43)
    val transitionedAt: Any? = this.transitionedAt
    result = result * prime + (transitionedAt?.hashCode() ?: 43)
    val metadata: Any? = this.metadata
    result = result * prime + (metadata?.hashCode() ?: 43)
    return result
  }

  class StreamStatusBuilder internal constructor() {
    private var id: UUID? = null
    private var workspaceId: UUID? = null
    private var connectionId: UUID? = null
    private var jobId: Long? = null
    private var attemptNumber: Int? = null
    private var streamNamespace: String? = null
    private var streamName: String? = null
    private var jobType: JobStreamStatusJobType? = null
    private var createdAt: OffsetDateTime? = null
    private var updatedAt: OffsetDateTime? = null
    private var runState: JobStreamStatusRunState? = null
    private var incompleteRunCause: JobStreamStatusIncompleteRunCause? = null
    private var transitionedAt: OffsetDateTime? = null
    private var metadata: JsonNode? = null

    fun id(id: UUID?): StreamStatusBuilder {
      this.id = id
      return this
    }

    fun workspaceId(workspaceId: UUID?): StreamStatusBuilder {
      this.workspaceId = workspaceId
      return this
    }

    fun connectionId(connectionId: UUID?): StreamStatusBuilder {
      this.connectionId = connectionId
      return this
    }

    fun jobId(jobId: Long?): StreamStatusBuilder {
      this.jobId = jobId
      return this
    }

    fun attemptNumber(attemptNumber: Int?): StreamStatusBuilder {
      this.attemptNumber = attemptNumber
      return this
    }

    fun streamNamespace(streamNamespace: String?): StreamStatusBuilder {
      this.streamNamespace = streamNamespace
      return this
    }

    fun streamName(streamName: String?): StreamStatusBuilder {
      this.streamName = streamName
      return this
    }

    fun jobType(jobType: JobStreamStatusJobType?): StreamStatusBuilder {
      this.jobType = jobType
      return this
    }

    fun createdAt(createdAt: OffsetDateTime?): StreamStatusBuilder {
      this.createdAt = createdAt
      return this
    }

    fun updatedAt(updatedAt: OffsetDateTime?): StreamStatusBuilder {
      this.updatedAt = updatedAt
      return this
    }

    fun runState(runState: JobStreamStatusRunState?): StreamStatusBuilder {
      this.runState = runState
      return this
    }

    fun incompleteRunCause(incompleteRunCause: JobStreamStatusIncompleteRunCause?): StreamStatusBuilder {
      this.incompleteRunCause = incompleteRunCause
      return this
    }

    fun transitionedAt(transitionedAt: OffsetDateTime?): StreamStatusBuilder {
      this.transitionedAt = transitionedAt
      return this
    }

    fun metadata(metadata: JsonNode?): StreamStatusBuilder {
      this.metadata = metadata
      return this
    }

    fun build(): StreamStatus {
      return StreamStatus(
        this.id, this.workspaceId, this.connectionId, this.jobId, this.attemptNumber, this.streamNamespace, this.streamName,
        this.jobType, this.createdAt, this.updatedAt, this.runState, this.incompleteRunCause, this.transitionedAt, this.metadata,
      )
    }

    override fun toString(): String {
      return (
        "StreamStatus.StreamStatusBuilder(id=" + this.id + ", workspaceId=" + this.workspaceId + ", connectionId=" + this.connectionId +
          ", jobId=" + this.jobId + ", attemptNumber=" + this.attemptNumber + ", streamNamespace=" + this.streamNamespace + ", streamName=" +
          this.streamName + ", jobType=" + this.jobType + ", createdAt=" + this.createdAt + ", updatedAt=" + this.updatedAt + ", runState=" +
          this.runState + ", incompleteRunCause=" + this.incompleteRunCause + ", transitionedAt=" + this.transitionedAt + ", metadata=" +
          this.metadata + ")"
      )
    }
  }

  companion object {
    fun builder(): StreamStatusBuilder {
      return StreamStatusBuilder()
    }
  }
}
