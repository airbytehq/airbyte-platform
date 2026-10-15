/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.StreamStatusCreateRequestBody
import io.airbyte.api.server.generated.models.StreamStatusIncompleteRunCause
import io.airbyte.api.server.generated.models.StreamStatusJobType
import io.airbyte.api.server.generated.models.StreamStatusListRequestBody
import io.airbyte.api.server.generated.models.StreamStatusRateLimitedMetadata
import io.airbyte.api.server.generated.models.StreamStatusRead
import io.airbyte.api.server.generated.models.StreamStatusRunState
import io.airbyte.api.server.generated.models.StreamStatusUpdateRequestBody
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

    api.metadata?.let { domain.metadata(map(it)) }

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

    api.metadata?.let { domain.metadata(map(it)) }

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
      // the controller rejects a pagination without these values before mapping
      val pageSize = api.pageSize!!
      StreamStatusesRepository.Pagination(
        api.rowOffset!! / pageSize,
        pageSize,
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
  // The required values are NOT NULL columns, so a persisted row always has them.
  fun map(domain: StreamStatus): StreamStatusRead =
    StreamStatusRead(
      id = domain.id!!,
      connectionId = domain.connectionId!!,
      workspaceId = domain.workspaceId!!,
      jobId = domain.jobId!!,
      jobType = map(domain.jobType)!!,
      attemptNumber = domain.attemptNumber!!,
      streamName = domain.streamName!!,
      streamNamespace = domain.streamNamespace,
      transitionedAt = domain.transitionedAt!!.toInstant().toEpochMilli(),
      runState = map(domain.runState)!!,
      incompleteRunCause = domain.incompleteRunCause?.let { map(it) },
      metadata = domain.metadata?.let { map(it) },
    )

  fun map(domainEnum: JobStreamStatusJobType?): StreamStatusJobType? =
    if (domainEnum != null) StreamStatusJobType.valueOf(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(domainEnum: JobStreamStatusRunState?): StreamStatusRunState? =
    if (domainEnum != null) StreamStatusRunState.valueOf(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(domainEnum: JobStreamStatusIncompleteRunCause?): StreamStatusIncompleteRunCause? =
    if (domainEnum != null) StreamStatusIncompleteRunCause.valueOf(domainEnum.name.uppercase(Locale.getDefault())) else null

  fun map(rateLimitedMetadata: JsonNode): StreamStatusRateLimitedMetadata {
    val rateLimitedInfo =
      Jsons.`object`(
        rateLimitedMetadata,
        StreamStatusRateLimitedMetadataRepositoryStructure::class.java,
      )
    return StreamStatusRateLimitedMetadata(quotaReset = rateLimitedInfo.quotaReset)
  }

  fun map(rateLimitedMetadata: StreamStatusRateLimitedMetadata): JsonNode {
    val streamStatusRateLimitedMetadataRepositoryStructure =
      StreamStatusRateLimitedMetadataRepositoryStructure(rateLimitedMetadata.quotaReset!!)
    return Jsons.jsonNode(streamStatusRateLimitedMetadataRepositoryStructure)
  }

  fun fromMills(millis: Long): OffsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
}
