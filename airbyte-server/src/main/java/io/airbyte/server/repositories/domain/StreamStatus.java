/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import java.util.UUID;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * DTO for our data access layer.
 */
@Builder
@Getter
@EqualsAndHashCode(exclude = {"id", "createdAt", "updatedAt"})
@MappedEntity("stream_statuses")
public class StreamStatus {

  @Id
  @AutoPopulated
  private UUID id;

  private UUID workspaceId;

  private UUID connectionId;

  private Long jobId;

  private Integer attemptNumber;

  @Nullable
  private String streamNamespace;

  private String streamName;

  @TypeDef(type = DataType.OBJECT)
  private JobStreamStatusJobType jobType;

  @DateCreated
  private OffsetDateTime createdAt;

  @DateUpdated
  private OffsetDateTime updatedAt;

  @TypeDef(type = DataType.OBJECT)
  private JobStreamStatusRunState runState;

  @Nullable
  @TypeDef(type = DataType.OBJECT)
  private JobStreamStatusIncompleteRunCause incompleteRunCause;

  private OffsetDateTime transitionedAt;

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

}
