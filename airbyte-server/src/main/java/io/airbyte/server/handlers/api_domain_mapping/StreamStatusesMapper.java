/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import static java.time.ZoneOffset.UTC;

import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusJobType;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.airbyte.server.repositories.StreamStatusesRepository;
import io.airbyte.server.repositories.domain.StreamStatus;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Maps between the API and Persistence layers. It is not static to be injectable and enable easier
 * testing in dependents.
 */
@Singleton
public class StreamStatusesMapper {

  // API to Domain
  public StreamStatus map(final StreamStatusCreateRequestBody api) {
    final var domain = StreamStatus.builder()
        .runState(map(api.getRunState()))
        .transitionedAt(fromMills(api.getTransitionedAt()))
        .workspaceId(api.getWorkspaceId())
        .connectionId(api.getConnectionId())
        .jobId(api.getJobId())
        .jobType(map(api.getJobType()))
        .attemptNumber(api.getAttemptNumber())
        .streamNamespace(api.getStreamNamespace())
        .streamName(api.getStreamName());

    if (null != api.getIncompleteRunCause()) {
      domain.incompleteRunCause(map(api.getIncompleteRunCause()));
    }

    return domain.build();
  }

  public StreamStatus map(final StreamStatusUpdateRequestBody api) {
    final var domain = StreamStatus.builder()
        .runState(map(api.getRunState()))
        .transitionedAt(fromMills(api.getTransitionedAt()))
        .workspaceId(api.getWorkspaceId())
        .connectionId(api.getConnectionId())
        .jobId(api.getJobId())
        .jobType(map(api.getJobType()))
        .attemptNumber(api.getAttemptNumber())
        .streamNamespace(api.getStreamNamespace())
        .streamName(api.getStreamName())
        .id(api.getId());

    if (null != api.getIncompleteRunCause()) {
      domain.incompleteRunCause(map(api.getIncompleteRunCause()));
    }

    return domain.build();
  }

  public JobStreamStatusJobType map(final StreamStatusJobType apiEnum) {
    return JobStreamStatusJobType.lookupLiteral(apiEnum.name().toLowerCase());
  }

  public JobStreamStatusRunState map(final StreamStatusRunState apiEnum) {
    return JobStreamStatusRunState.lookupLiteral(apiEnum.name().toLowerCase());
  }

  public JobStreamStatusIncompleteRunCause map(final StreamStatusIncompleteRunCause apiEnum) {
    return JobStreamStatusIncompleteRunCause.lookupLiteral(apiEnum.name().toLowerCase());
  }

  public StreamStatusesRepository.Pagination map(final Pagination api) {
    return new StreamStatusesRepository.Pagination(
        api.getRowOffset() / api.getPageSize(),
        api.getPageSize());
  }

  public StreamStatusesRepository.FilterParams map(final StreamStatusListRequestBody api) {
    final var domain = StreamStatusesRepository.FilterParams.builder()
        .workspaceId(api.getWorkspaceId())
        .connectionId(api.getConnectionId())
        .jobId(api.getJobId())
        .attemptNumber(api.getAttemptNumber())
        .streamName(api.getStreamName())
        .streamNamespace(api.getStreamNamespace())
        .pagination(map(api.getPagination()));

    if (null != api.getJobType()) {
      domain.jobType(map(api.getJobType()));
    }

    return domain.build();
  }

  // Domain to API
  public StreamStatusRead map(final StreamStatus domain) {
    final var api = new StreamStatusRead();
    api.setId(domain.getId());
    api.setConnectionId(domain.getConnectionId());
    api.setWorkspaceId(domain.getWorkspaceId());
    api.setJobId(domain.getJobId());
    api.setJobType(map(domain.getJobType()));
    api.setAttemptNumber(domain.getAttemptNumber());
    api.setStreamName(domain.getStreamName());
    api.setStreamNamespace(domain.getStreamNamespace());
    api.setTransitionedAt(domain.getTransitionedAt().toInstant().toEpochMilli());
    api.setRunState(map(domain.getRunState()));

    if (null != domain.getIncompleteRunCause()) {
      api.setIncompleteRunCause(map(domain.getIncompleteRunCause()));
    }

    return api;
  }

  public StreamStatusJobType map(final JobStreamStatusJobType domainEnum) {
    return StreamStatusJobType.fromValue(domainEnum.name().toUpperCase());
  }

  public StreamStatusRunState map(final JobStreamStatusRunState domainEnum) {
    return StreamStatusRunState.fromValue(domainEnum.name().toUpperCase());
  }

  public StreamStatusIncompleteRunCause map(final JobStreamStatusIncompleteRunCause domainEnum) {
    return StreamStatusIncompleteRunCause.fromValue(domainEnum.name().toUpperCase());
  }

  OffsetDateTime fromMills(final Long millis) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
  }

}
