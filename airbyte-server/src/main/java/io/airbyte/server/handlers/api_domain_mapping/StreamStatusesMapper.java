/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import static java.time.ZoneOffset.UTC;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusJobType;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRateLimitedMetadata;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.airbyte.server.repositories.StreamStatusesRepository;
import io.airbyte.server.repositories.domain.StreamStatus;
import io.airbyte.server.repositories.domain.StreamStatusRateLimitedMetadataRepositoryStructure;
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
    final var domain = new StreamStatus.StreamStatusBuilder()
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

    if (null != api.getMetadata()) {
      domain.metadata(map(api.getMetadata()));
    }

    return domain.build();
  }

  public StreamStatus map(final StreamStatusUpdateRequestBody api) {
    final var domain = new StreamStatus.StreamStatusBuilder()
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

    if (null != api.getMetadata()) {
      domain.metadata(map(api.getMetadata()));
    }

    return domain.build();
  }

  public JobStreamStatusJobType map(final StreamStatusJobType apiEnum) {
    return apiEnum != null ? JobStreamStatusJobType.lookupLiteral(apiEnum.name().toLowerCase()) : null;
  }

  public JobStreamStatusRunState map(final StreamStatusRunState apiEnum) {
    return apiEnum != null ? JobStreamStatusRunState.lookupLiteral(apiEnum.name().toLowerCase()) : null;
  }

  public JobStreamStatusIncompleteRunCause map(final StreamStatusIncompleteRunCause apiEnum) {
    return apiEnum != null ? JobStreamStatusIncompleteRunCause.lookupLiteral(apiEnum.name().toLowerCase()) : null;
  }

  public StreamStatusesRepository.Pagination map(final Pagination api) {
    return api != null ? new StreamStatusesRepository.Pagination(
        api.getRowOffset() / api.getPageSize(),
        api.getPageSize()) : null;
  }

  public StreamStatusesRepository.FilterParams map(final StreamStatusListRequestBody api) {
    return new StreamStatusesRepository.FilterParams(
        api.getWorkspaceId(),
        api.getConnectionId(),
        api.getJobId(),
        api.getStreamNamespace(),
        api.getStreamName(),
        api.getAttemptNumber(),
        map(api.getJobType()),
        map(api.getPagination()));
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

    if (null != domain.getMetadata()) {
      api.setMetadata(map(domain.getMetadata()));
    }

    return api;
  }

  public StreamStatusJobType map(final JobStreamStatusJobType domainEnum) {
    return domainEnum != null ? StreamStatusJobType.fromValue(domainEnum.name().toUpperCase()) : null;
  }

  public StreamStatusRunState map(final JobStreamStatusRunState domainEnum) {
    return domainEnum != null ? StreamStatusRunState.fromValue(domainEnum.name().toUpperCase()) : null;
  }

  public StreamStatusIncompleteRunCause map(final JobStreamStatusIncompleteRunCause domainEnum) {
    return domainEnum != null ? StreamStatusIncompleteRunCause.fromValue(domainEnum.name().toUpperCase()) : null;
  }

  public StreamStatusRateLimitedMetadata map(final JsonNode rateLimitedMetadata) {
    final StreamStatusRateLimitedMetadataRepositoryStructure rateLimitedInfo =
        Jsons.object(rateLimitedMetadata, StreamStatusRateLimitedMetadataRepositoryStructure.class);
    return new StreamStatusRateLimitedMetadata().quotaReset(rateLimitedInfo.getQuotaReset());
  }

  public JsonNode map(final StreamStatusRateLimitedMetadata rateLimitedMetadata) {
    final StreamStatusRateLimitedMetadataRepositoryStructure streamStatusRateLimitedMetadataRepositoryStructure =
        new StreamStatusRateLimitedMetadataRepositoryStructure(rateLimitedMetadata.getQuotaReset());
    return Jsons.jsonNode(streamStatusRateLimitedMetadataRepositoryStructure);
  }

  OffsetDateTime fromMills(final Long millis) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
  }

}
