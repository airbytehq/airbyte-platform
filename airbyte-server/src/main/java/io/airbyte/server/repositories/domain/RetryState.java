/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain;

import io.micronaut.data.annotation.AutoPopulated;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * DTO for our data access layer.
 */
@MappedEntity("retry_states")
public class RetryState {

  @Id
  @AutoPopulated
  private final UUID id;

  private final UUID connectionId;

  private final Long jobId;

  @DateCreated
  private final OffsetDateTime createdAt;

  @DateUpdated
  private final OffsetDateTime updatedAt;

  private final Integer successiveCompleteFailures;

  private final Integer totalCompleteFailures;

  private final Integer successivePartialFailures;

  private final Integer totalPartialFailures;

  public RetryState(UUID id,
                    UUID connectionId,
                    Long jobId,
                    OffsetDateTime createdAt,
                    OffsetDateTime updatedAt,
                    Integer successiveCompleteFailures,
                    Integer totalCompleteFailures,
                    Integer successivePartialFailures,
                    Integer totalPartialFailures) {
    this.id = id;
    this.connectionId = connectionId;
    this.jobId = jobId;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.successiveCompleteFailures = successiveCompleteFailures;
    this.totalCompleteFailures = totalCompleteFailures;
    this.successivePartialFailures = successivePartialFailures;
    this.totalPartialFailures = totalPartialFailures;
  }

  public static RetryStateBuilder builder() {
    return new RetryStateBuilder();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof final RetryState other)) {
      return false;
    }
    if (!other.canEqual(this)) {
      return false;
    }
    final Object thisConnectionId = this.getConnectionId();
    final Object otherConnectionId = other.getConnectionId();
    if (!Objects.equals(thisConnectionId, otherConnectionId)) {
      return false;
    }
    final Object thisJobId = this.getJobId();
    final Object otherJobId = other.getJobId();
    if (!Objects.equals(thisJobId, otherJobId)) {
      return false;
    }
    final Object thisSuccessiveCompleteFailures = this.getSuccessiveCompleteFailures();
    final Object otherSuccessiveCompleteFailures = other.getSuccessiveCompleteFailures();
    if (!Objects.equals(thisSuccessiveCompleteFailures, otherSuccessiveCompleteFailures)) {
      return false;
    }
    final Object thisTotalCompleteFailures = this.getTotalCompleteFailures();
    final Object otherTotalCompleteFailures = other.getTotalCompleteFailures();
    if (!Objects.equals(thisTotalCompleteFailures, otherTotalCompleteFailures)) {
      return false;
    }
    final Object thisSuccessivePartialFailures = this.getSuccessivePartialFailures();
    final Object otherSuccessivePartialFailures = other.getSuccessivePartialFailures();
    if (!Objects.equals(thisSuccessivePartialFailures, otherSuccessivePartialFailures)) {
      return false;
    }
    final Object thisTotalPartialFailures = this.getTotalPartialFailures();
    final Object otherTotalPartialFailures = other.getTotalPartialFailures();
    return Objects.equals(thisTotalPartialFailures, otherTotalPartialFailures);
  }

  protected boolean canEqual(final Object other) {
    return other instanceof RetryState;
  }

  @Override
  public int hashCode() {
    final int prime = 59;
    int result = 1;
    final Object connectionId = this.getConnectionId();
    result = result * prime + (connectionId == null ? 43 : connectionId.hashCode());
    final Object jobId = this.getJobId();
    result = result * prime + (jobId == null ? 43 : jobId.hashCode());
    final Object successiveCompleteFailures = this.getSuccessiveCompleteFailures();
    result = result * prime + (successiveCompleteFailures == null ? 43 : successiveCompleteFailures.hashCode());
    final Object totalCompleteFailures = this.getTotalCompleteFailures();
    result = result * prime + (totalCompleteFailures == null ? 43 : totalCompleteFailures.hashCode());
    final Object successivePartialFailures = this.getSuccessivePartialFailures();
    result = result * prime + (successivePartialFailures == null ? 43 : successivePartialFailures.hashCode());
    final Object totalPartialFailures = this.getTotalPartialFailures();
    result = result * prime + (totalPartialFailures == null ? 43 : totalPartialFailures.hashCode());
    return result;
  }

  public UUID getId() {
    return this.id;
  }

  public UUID getConnectionId() {
    return this.connectionId;
  }

  public Long getJobId() {
    return this.jobId;
  }

  public OffsetDateTime getCreatedAt() {
    return this.createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return this.updatedAt;
  }

  public Integer getSuccessiveCompleteFailures() {
    return this.successiveCompleteFailures;
  }

  public Integer getTotalCompleteFailures() {
    return this.totalCompleteFailures;
  }

  public Integer getSuccessivePartialFailures() {
    return this.successivePartialFailures;
  }

  public Integer getTotalPartialFailures() {
    return this.totalPartialFailures;
  }

  public static class RetryStateBuilder {

    private UUID id;
    private UUID connectionId;
    private Long jobId;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private Integer successiveCompleteFailures;
    private Integer totalCompleteFailures;
    private Integer successivePartialFailures;
    private Integer totalPartialFailures;

    RetryStateBuilder() {}

    public RetryStateBuilder id(UUID id) {
      this.id = id;
      return this;
    }

    public RetryStateBuilder connectionId(UUID connectionId) {
      this.connectionId = connectionId;
      return this;
    }

    public RetryStateBuilder jobId(Long jobId) {
      this.jobId = jobId;
      return this;
    }

    public RetryStateBuilder createdAt(OffsetDateTime createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public RetryStateBuilder updatedAt(OffsetDateTime updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public RetryStateBuilder successiveCompleteFailures(Integer successiveCompleteFailures) {
      this.successiveCompleteFailures = successiveCompleteFailures;
      return this;
    }

    public RetryStateBuilder totalCompleteFailures(Integer totalCompleteFailures) {
      this.totalCompleteFailures = totalCompleteFailures;
      return this;
    }

    public RetryStateBuilder successivePartialFailures(Integer successivePartialFailures) {
      this.successivePartialFailures = successivePartialFailures;
      return this;
    }

    public RetryStateBuilder totalPartialFailures(Integer totalPartialFailures) {
      this.totalPartialFailures = totalPartialFailures;
      return this;
    }

    public RetryState build() {
      return new RetryState(this.id, this.connectionId, this.jobId, this.createdAt, this.updatedAt, this.successiveCompleteFailures,
          this.totalCompleteFailures, this.successivePartialFailures, this.totalPartialFailures);
    }

    @Override
    public String toString() {
      return "RetryState.RetryStateBuilder(id=" + this.id + ", connectionId=" + this.connectionId + ", jobId=" + this.jobId + ", createdAt="
          + this.createdAt + ", updatedAt=" + this.updatedAt + ", successiveCompleteFailures=" + this.successiveCompleteFailures
          + ", totalCompleteFailures=" + this.totalCompleteFailures + ", successivePartialFailures=" + this.successivePartialFailures
          + ", totalPartialFailures=" + this.totalPartialFailures + ")";
    }

  }

}
