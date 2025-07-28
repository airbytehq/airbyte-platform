/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.model.generated.StreamStatusJobType
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusRunState
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.server.repositories.StreamStatusesRepository
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams
import io.airbyte.server.repositories.domain.StreamStatus
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder
import io.airbyte.server.repositories.domain.StreamStatusRateLimitedMetadataRepositoryStructure
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Locale

/**
 * Maps between the API and Persistence layers. It is not static to be injectable and enable easier
 * testing in dependents.
 */
@Singleton
class StreamStatusesMapper {
  // API to Domain
  fun map(api: StreamStatusCreateRequestBody): StreamStatus {
    val domain =
      StreamStatusBuilder()
        .runState(map(api.runState))
        .transitionedAt(fromMills(api.transitionedAt))
        .workspaceId(api.workspaceId)
        .connectionId(api.connectionId)
        .jobId(api.jobId)
        .jobType(map(api.jobType))
        .attemptNumber(api.attemptNumber)
        .streamNamespace(api.streamNamespace)
        .streamName(api.streamName)

    if (null != api.incompleteRunCause) {
      domain.incompleteRunCause(map(api.incompleteRunCause))
    }

    if (null != api.metadata) {
      domain.metadata(map(api.metadata))
    }

    return domain.build()
  }

  fun map(api: StreamStatusUpdateRequestBody): StreamStatus {
    val domain =
      StreamStatusBuilder()
        .runState(map(api.runState))
        .transitionedAt(fromMills(api.transitionedAt))
        .workspaceId(api.workspaceId)
        .connectionId(api.connectionId)
        .jobId(api.jobId)
        .jobType(map(api.jobType))
        .attemptNumber(api.attemptNumber)
        .streamNamespace(api.streamNamespace)
        .streamName(api.streamName)
        .id(api.id)

    if (null != api.incompleteRunCause) {
      domain.incompleteRunCause(map(api.incompleteRunCause))
    }

    if (null != api.metadata) {
      domain.metadata(map(api.metadata))
    }

    return domain.build()
  }

  fun map(apiEnum: StreamStatusJobType?): JobStreamStatusJobType? =
    if (apiEnum != null) JobStreamStatusJobType.lookupLiteral(apiEnum.name.lowercase(Locale.getDefault())) else null

  fun map(apiEnum: StreamStatusRunState?): JobStreamStatusRunState? =
    if (apiEnum != null) JobStreamStatusRunState.lookupLiteral(apiEnum.name.lowercase(Locale.getDefault())) else null

  fun map(apiEnum: StreamStatusIncompleteRunCause?): JobStreamStatusIncompleteRunCause? =
    if (apiEnum != null) JobStreamStatusIncompleteRunCause.lookupLiteral(apiEnum.name.lowercase(Locale.getDefault())) else null

  fun map(api: Pagination?): StreamStatusesRepository.Pagination? =
    if (api != null) {
      StreamStatusesRepository.Pagination(
        api.rowOffset / api.pageSize,
        api.pageSize,
      )
    } else {
      null
    }

  fun map(api: StreamStatusListRequestBody): FilterParams =
    FilterParams(
      api.workspaceId,
      api.connectionId,
      api.jobId,
      api.streamNamespace,
      api.streamName,
      api.attemptNumber,
      map(api.jobType),
      map(api.pagination),
    )

  // Domain to API
  fun map(domain: StreamStatus): StreamStatusRead {
    val api = StreamStatusRead()
    api.id = domain.id
    api.connectionId = domain.connectionId
    api.workspaceId = domain.workspaceId
    api.jobId = domain.jobId
    api.jobType = map(domain.jobType)
    api.attemptNumber = domain.attemptNumber
    api.streamName = domain.streamName
    api.streamNamespace = domain.streamNamespace
    api.transitionedAt = domain.transitionedAt!!.toInstant().toEpochMilli()
    api.runState = map(domain.runState)

    if (null != domain.incompleteRunCause) {
      api.incompleteRunCause = map(domain.incompleteRunCause)
    }

    if (null != domain.metadata) {
      api.metadata = map(domain.metadata)
    }

    return api
  }

  fun map(domainEnum: JobStreamStatusJobType?): StreamStatusJobType? =
    if (domainEnum != null) StreamStatusJobType.fromValue(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(domainEnum: JobStreamStatusRunState?): StreamStatusRunState? =
    if (domainEnum != null) StreamStatusRunState.fromValue(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(domainEnum: JobStreamStatusIncompleteRunCause?): StreamStatusIncompleteRunCause? =
    if (domainEnum != null) StreamStatusIncompleteRunCause.fromValue(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(rateLimitedMetadata: JsonNode): StreamStatusRateLimitedMetadata {
    val rateLimitedInfo =
      Jsons.`object`(
        rateLimitedMetadata,
        StreamStatusRateLimitedMetadataRepositoryStructure::class.java,
      )
    return StreamStatusRateLimitedMetadata().quotaReset(rateLimitedInfo.quotaReset)
  }

  fun map(rateLimitedMetadata: StreamStatusRateLimitedMetadata): JsonNode {
    val streamStatusRateLimitedMetadataRepositoryStructure =
      StreamStatusRateLimitedMetadataRepositoryStructure(rateLimitedMetadata.quotaReset)
    return Jsons.jsonNode(streamStatusRateLimitedMetadataRepositoryStructure)
  }

  fun fromMills(millis: Long): OffsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
}
