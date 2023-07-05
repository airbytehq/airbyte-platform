/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain;

import io.micronaut.data.annotation.AutoPopulated;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * DTO for our data access layer.
 */
@Builder(toBuilder = true)
@AllArgsConstructor
@Getter
@EqualsAndHashCode(exclude = {"id", "createdAt", "updatedAt"})
@MappedEntity("retry_states")
public class RetryState {

  @Id
  @AutoPopulated
  private UUID id;

  private UUID connectionId;

  private Long jobId;

  @DateCreated
  private OffsetDateTime createdAt;

  @DateUpdated
  private OffsetDateTime updatedAt;

  private Integer successiveCompleteFailures;

  private Integer totalCompleteFailures;

  private Integer successivePartialFailures;

  private Integer totalPartialFailures;

}
