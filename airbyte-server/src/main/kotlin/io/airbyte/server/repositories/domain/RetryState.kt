/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

/**
 * DTO for our data access layer.
 */
@MappedEntity("retry_states")
class RetryState(
  @field:AutoPopulated @field:Id val id: UUID?,
  val connectionId: UUID?,
  val jobId: Long?,
  @field:DateCreated val createdAt: OffsetDateTime?,
  @field:DateUpdated val updatedAt: OffsetDateTime?,
  val successiveCompleteFailures: Int?,
  val totalCompleteFailures: Int?,
  val successivePartialFailures: Int?,
  val totalPartialFailures: Int?,
) {
  override fun equals(other: Any?): Boolean {
    if (other === this) {
      return true
    }
    if (other !is RetryState) {
      return false
    }
    if (!other.canEqual(this)) {
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
    val thisSuccessiveCompleteFailures: Any? = this.successiveCompleteFailures
    val otherSuccessiveCompleteFailures: Any? = other.successiveCompleteFailures
    if (thisSuccessiveCompleteFailures != otherSuccessiveCompleteFailures) {
      return false
    }
    val thisTotalCompleteFailures: Any? = this.totalCompleteFailures
    val otherTotalCompleteFailures: Any? = other.totalCompleteFailures
    if (thisTotalCompleteFailures != otherTotalCompleteFailures) {
      return false
    }
    val thisSuccessivePartialFailures: Any? = this.successivePartialFailures
    val otherSuccessivePartialFailures: Any? = other.successivePartialFailures
    if (thisSuccessivePartialFailures != otherSuccessivePartialFailures) {
      return false
    }
    val thisTotalPartialFailures: Any? = this.totalPartialFailures
    val otherTotalPartialFailures: Any? = other.totalPartialFailures
    return thisTotalPartialFailures == otherTotalPartialFailures
  }

  private fun canEqual(other: Any?): Boolean {
    return other is RetryState
  }

  override fun hashCode(): Int {
    val prime = 59
    var result = 1
    val connectionId: Any? = this.connectionId
    result = result * prime + (connectionId?.hashCode() ?: 43)
    val jobId: Any? = this.jobId
    result = result * prime + (jobId?.hashCode() ?: 43)
    val successiveCompleteFailures: Any? = this.successiveCompleteFailures
    result = result * prime + (successiveCompleteFailures?.hashCode() ?: 43)
    val totalCompleteFailures: Any? = this.totalCompleteFailures
    result = result * prime + (totalCompleteFailures?.hashCode() ?: 43)
    val successivePartialFailures: Any? = this.successivePartialFailures
    result = result * prime + (successivePartialFailures?.hashCode() ?: 43)
    val totalPartialFailures: Any? = this.totalPartialFailures
    result = result * prime + (totalPartialFailures?.hashCode() ?: 43)
    return result
  }

  class RetryStateBuilder internal constructor() {
    private var id: UUID? = null
    private var connectionId: UUID? = null
    private var jobId: Long? = null
    private var createdAt: OffsetDateTime? = null
    private var updatedAt: OffsetDateTime? = null
    private var successiveCompleteFailures: Int? = null
    private var totalCompleteFailures: Int? = null
    private var successivePartialFailures: Int? = null
    private var totalPartialFailures: Int? = null

    fun id(id: UUID?): RetryStateBuilder {
      this.id = id
      return this
    }

    fun connectionId(connectionId: UUID?): RetryStateBuilder {
      this.connectionId = connectionId
      return this
    }

    fun jobId(jobId: Long?): RetryStateBuilder {
      this.jobId = jobId
      return this
    }

    fun createdAt(createdAt: OffsetDateTime?): RetryStateBuilder {
      this.createdAt = createdAt
      return this
    }

    fun updatedAt(updatedAt: OffsetDateTime?): RetryStateBuilder {
      this.updatedAt = updatedAt
      return this
    }

    fun successiveCompleteFailures(successiveCompleteFailures: Int?): RetryStateBuilder {
      this.successiveCompleteFailures = successiveCompleteFailures
      return this
    }

    fun totalCompleteFailures(totalCompleteFailures: Int?): RetryStateBuilder {
      this.totalCompleteFailures = totalCompleteFailures
      return this
    }

    fun successivePartialFailures(successivePartialFailures: Int?): RetryStateBuilder {
      this.successivePartialFailures = successivePartialFailures
      return this
    }

    fun totalPartialFailures(totalPartialFailures: Int?): RetryStateBuilder {
      this.totalPartialFailures = totalPartialFailures
      return this
    }

    fun build(): RetryState {
      return RetryState(
        id = this.id, connectionId = this.connectionId, jobId = this.jobId, createdAt = this.createdAt, updatedAt = this.updatedAt,
        successiveCompleteFailures = this.successiveCompleteFailures,
        totalCompleteFailures = this.totalCompleteFailures,
        successivePartialFailures = this.successivePartialFailures,
        totalPartialFailures = this.totalPartialFailures,
      )
    }

    override fun toString(): String {
      return (
        "RetryState.RetryStateBuilder(id=" + this.id + ", connectionId=" + this.connectionId + ", jobId=" + this.jobId + ", createdAt=" +
          this.createdAt + ", updatedAt=" + this.updatedAt + ", successiveCompleteFailures=" + this.successiveCompleteFailures +
          ", totalCompleteFailures=" + this.totalCompleteFailures + ", successivePartialFailures=" + this.successivePartialFailures +
          ", totalPartialFailures=" + this.totalPartialFailures + ")"
      )
    }
  }

  companion object {
    fun builder(): RetryStateBuilder {
      return RetryStateBuilder()
    }
  }
}
