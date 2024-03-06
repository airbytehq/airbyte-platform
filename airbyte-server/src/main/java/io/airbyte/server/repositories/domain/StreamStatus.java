/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain;

import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.annotation.AutoPopulated;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * DTO for our data access layer.
 */
@MappedEntity("stream_statuses")
public class StreamStatus {

  @Id
  @AutoPopulated
  private final UUID id;

  private final UUID workspaceId;

  private final UUID connectionId;

  private final Long jobId;

  private final Integer attemptNumber;

  @Nullable
  private final String streamNamespace;

  private final String streamName;

  @TypeDef(type = DataType.OBJECT)
  private final JobStreamStatusJobType jobType;

  @DateCreated
  private final OffsetDateTime createdAt;

  @DateUpdated
  private final OffsetDateTime updatedAt;

  @TypeDef(type = DataType.OBJECT)
  private final JobStreamStatusRunState runState;

  @Nullable
  @TypeDef(type = DataType.OBJECT)
  private final JobStreamStatusIncompleteRunCause incompleteRunCause;

  private final OffsetDateTime transitionedAt;

  public StreamStatus(final UUID id,
                      final UUID workspaceId,
                      final UUID connectionId,
                      final Long jobId,
                      final Integer attemptNumber,
                      final @Nullable String streamNamespace,
                      final String streamName,
                      final JobStreamStatusJobType jobType,
                      final OffsetDateTime createdAt,
                      final OffsetDateTime updatedAt,
                      final JobStreamStatusRunState runState,
                      final @Nullable JobStreamStatusIncompleteRunCause incompleteRunCause,
                      final OffsetDateTime transitionedAt) {
    this.id = id;
    this.workspaceId = workspaceId;
    this.connectionId = connectionId;
    this.jobId = jobId;
    this.attemptNumber = attemptNumber;
    this.streamNamespace = streamNamespace;
    this.streamName = streamName;
    this.jobType = jobType;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.runState = runState;
    this.incompleteRunCause = incompleteRunCause;
    this.transitionedAt = transitionedAt;
  }

  public static StreamStatusBuilder builder() {
    return new StreamStatusBuilder();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof final StreamStatus other)) {
      return false;
    }
    if (!other.canEqual(this)) {
      return false;
    }
    final Object thisWorkspaceId = this.getWorkspaceId();
    final Object otherWorkspaceId = other.getWorkspaceId();
    if (!Objects.equals(thisWorkspaceId, otherWorkspaceId)) {
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
    final Object thisAttemptNumber = this.getAttemptNumber();
    final Object otherAttemptNumber = other.getAttemptNumber();
    if (!Objects.equals(thisAttemptNumber, otherAttemptNumber)) {
      return false;
    }
    final Object thisStreamNamespace = this.getStreamNamespace();
    final Object otherStreamNamespace = other.getStreamNamespace();
    if (!Objects.equals(thisStreamNamespace, otherStreamNamespace)) {
      return false;
    }
    final Object thisStreamName = this.getStreamName();
    final Object otherStreamName = other.getStreamName();
    if (!Objects.equals(thisStreamName, otherStreamName)) {
      return false;
    }
    final Object thisJobType = this.getJobType();
    final Object otherJobType = other.getJobType();
    if (!Objects.equals(thisJobType, otherJobType)) {
      return false;
    }
    final Object thisRunState = this.getRunState();
    final Object otherRunState = other.getRunState();
    if (!Objects.equals(thisRunState, otherRunState)) {
      return false;
    }
    final Object thisIncompleteRunCause = this.getIncompleteRunCause();
    final Object otherIncompleteRunCause = other.getIncompleteRunCause();
    if (!Objects.equals(thisIncompleteRunCause, otherIncompleteRunCause)) {
      return false;
    }
    final Object thisTransitionedAt = this.getTransitionedAt();
    final Object otherTransitionedAt = other.getTransitionedAt();
    return Objects.equals(thisTransitionedAt, otherTransitionedAt);
  }

  protected boolean canEqual(final Object other) {
    return other instanceof StreamStatus;
  }

  @Override
  public int hashCode() {
    final int prime = 59;
    int result = 1;
    final Object workspaceId = this.getWorkspaceId();
    result = result * prime + (workspaceId == null ? 43 : workspaceId.hashCode());
    final Object connectionId = this.getConnectionId();
    result = result * prime + (connectionId == null ? 43 : connectionId.hashCode());
    final Object jobId = this.getJobId();
    result = result * prime + (jobId == null ? 43 : jobId.hashCode());
    final Object attemptNumber = this.getAttemptNumber();
    result = result * prime + (attemptNumber == null ? 43 : attemptNumber.hashCode());
    final Object streamNamespace = this.getStreamNamespace();
    result = result * prime + (streamNamespace == null ? 43 : streamNamespace.hashCode());
    final Object streamName = this.getStreamName();
    result = result * prime + (streamName == null ? 43 : streamName.hashCode());
    final Object jobType = this.getJobType();
    result = result * prime + (jobType == null ? 43 : jobType.hashCode());
    final Object runState = this.getRunState();
    result = result * prime + (runState == null ? 43 : runState.hashCode());
    final Object incompleteRunCause = this.getIncompleteRunCause();
    result = result * prime + (incompleteRunCause == null ? 43 : incompleteRunCause.hashCode());
    final Object transitionedAt = this.getTransitionedAt();
    result = result * prime + (transitionedAt == null ? 43 : transitionedAt.hashCode());
    return result;
  }

  public UUID getId() {
    return this.id;
  }

  public UUID getWorkspaceId() {
    return this.workspaceId;
  }

  public UUID getConnectionId() {
    return this.connectionId;
  }

  public Long getJobId() {
    return this.jobId;
  }

  public Integer getAttemptNumber() {
    return this.attemptNumber;
  }

  public String getStreamNamespace() {
    return this.streamNamespace;
  }

  public String getStreamName() {
    return this.streamName;
  }

  public JobStreamStatusJobType getJobType() {
    return this.jobType;
  }

  public OffsetDateTime getCreatedAt() {
    return this.createdAt;
  }

  public OffsetDateTime getUpdatedAt() {
    return this.updatedAt;
  }

  public JobStreamStatusRunState getRunState() {
    return this.runState;
  }

  public JobStreamStatusIncompleteRunCause getIncompleteRunCause() {
    return this.incompleteRunCause;
  }

  public OffsetDateTime getTransitionedAt() {
    return this.transitionedAt;
  }

  public static class StreamStatusBuilder {

    private UUID id;
    private UUID workspaceId;
    private UUID connectionId;
    private Long jobId;
    private Integer attemptNumber;
    private String streamNamespace;
    private String streamName;
    private JobStreamStatusJobType jobType;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private JobStreamStatusRunState runState;
    private JobStreamStatusIncompleteRunCause incompleteRunCause;
    private OffsetDateTime transitionedAt;

    StreamStatusBuilder() {}

    public StreamStatusBuilder id(UUID id) {
      this.id = id;
      return this;
    }

    public StreamStatusBuilder workspaceId(UUID workspaceId) {
      this.workspaceId = workspaceId;
      return this;
    }

    public StreamStatusBuilder connectionId(UUID connectionId) {
      this.connectionId = connectionId;
      return this;
    }

    public StreamStatusBuilder jobId(Long jobId) {
      this.jobId = jobId;
      return this;
    }

    public StreamStatusBuilder attemptNumber(Integer attemptNumber) {
      this.attemptNumber = attemptNumber;
      return this;
    }

    public StreamStatusBuilder streamNamespace(String streamNamespace) {
      this.streamNamespace = streamNamespace;
      return this;
    }

    public StreamStatusBuilder streamName(String streamName) {
      this.streamName = streamName;
      return this;
    }

    public StreamStatusBuilder jobType(JobStreamStatusJobType jobType) {
      this.jobType = jobType;
      return this;
    }

    public StreamStatusBuilder createdAt(OffsetDateTime createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public StreamStatusBuilder updatedAt(OffsetDateTime updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public StreamStatusBuilder runState(JobStreamStatusRunState runState) {
      this.runState = runState;
      return this;
    }

    public StreamStatusBuilder incompleteRunCause(JobStreamStatusIncompleteRunCause incompleteRunCause) {
      this.incompleteRunCause = incompleteRunCause;
      return this;
    }

    public StreamStatusBuilder transitionedAt(OffsetDateTime transitionedAt) {
      this.transitionedAt = transitionedAt;
      return this;
    }

    public StreamStatus build() {
      return new StreamStatus(this.id, this.workspaceId, this.connectionId, this.jobId, this.attemptNumber, this.streamNamespace, this.streamName,
          this.jobType, this.createdAt, this.updatedAt, this.runState, this.incompleteRunCause, this.transitionedAt);
    }

    @Override
    public String toString() {
      return "StreamStatus.StreamStatusBuilder(id=" + this.id + ", workspaceId=" + this.workspaceId + ", connectionId=" + this.connectionId
          + ", jobId=" + this.jobId + ", attemptNumber=" + this.attemptNumber + ", streamNamespace=" + this.streamNamespace + ", streamName="
          + this.streamName + ", jobType=" + this.jobType + ", createdAt=" + this.createdAt + ", updatedAt=" + this.updatedAt + ", runState="
          + this.runState + ", incompleteRunCause=" + this.incompleteRunCause + ", transitionedAt=" + this.transitionedAt + ")";
    }

  }

}
