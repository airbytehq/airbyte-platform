/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

private val logger = KotlinLogging.logger {}

/**
 * Consumes StreamStatusUpdateEvents and emits a create or update request for the stream status depending on
 * the contents of its cache.
 *
 * API layer.
 */
@Singleton
class StreamStatusCachingApiClient(
  private val airbyteApiClient: AirbyteApiClient,
  private val clock: Clock,
) {
  fun put(
    cache: MutableMap<StreamStatusKey, StreamStatusRead>,
    key: StreamStatusKey,
    runState: ApiEnum,
    metadata: StreamStatusRateLimitedMetadata? = null,
    ctx: ReplicationContext,
  ) {
    logger.info { "Stream Status Update Received: ${key.toDisplayName()} - $runState" }

    val value = cache[key]

    if (value == null) {
      logger.info { "Creating status: ${key.toDisplayName()} - $runState" }
      val req = buildCreateReq(key.streamNamespace, key.streamName, ctx, runState, metadata)

      val resp = airbyteApiClient.streamStatusesApi.createStreamStatus(req)
      cache[key] = resp
    } else if (value.runState != runState) {
      logger.info { "Updating status: ${key.toDisplayName()} - $runState" }
      val req = buildUpdateReq(value.id, key.streamNamespace, key.streamName, ctx, runState, metadata)

      val resp = airbyteApiClient.streamStatusesApi.updateStreamStatus(req)
      cache[key] = resp
    } else {
      logger.info { "Stream ${key.toDisplayName()} is already set to $runState. Ignoring..." }
    }
  }

  @VisibleForTesting
  fun buildCreateReq(
    streamNamespace: String?,
    streamName: String,
    ctx: ReplicationContext,
    runState: ApiEnum,
    metadata: StreamStatusRateLimitedMetadata? = null,
  ): StreamStatusCreateRequestBody =
    StreamStatusCreateRequestBody(
      attemptNumber = ctx.attempt,
      connectionId = ctx.connectionId,
      jobId = ctx.jobId,
      jobType =
        if (ctx.isReset) {
          StreamStatusJobType.RESET
        } else {
          StreamStatusJobType.SYNC
        },
      runState = runState,
      streamName = streamName,
      transitionedAt = clock.millis(),
      workspaceId = ctx.workspaceId,
      incompleteRunCause =
        if (runState == ApiEnum.INCOMPLETE) {
          StreamStatusIncompleteRunCause.FAILED
        } else {
          null
        },
      streamNamespace = streamNamespace,
      metadata = metadata,
    )

  @VisibleForTesting
  fun buildUpdateReq(
    id: UUID,
    streamNamespace: String?,
    streamName: String,
    ctx: ReplicationContext,
    runState: ApiEnum,
    metadata: StreamStatusRateLimitedMetadata? = null,
  ): StreamStatusUpdateRequestBody =
    StreamStatusUpdateRequestBody(
      id = id,
      attemptNumber = ctx.attempt,
      connectionId = ctx.connectionId,
      jobId = ctx.jobId,
      jobType =
        if (ctx.isReset) {
          StreamStatusJobType.RESET
        } else {
          StreamStatusJobType.SYNC
        },
      runState = runState,
      streamName = streamName,
      transitionedAt = clock.millis(),
      workspaceId = ctx.workspaceId,
      incompleteRunCause =
        if (runState == ApiEnum.INCOMPLETE) {
          StreamStatusIncompleteRunCause.FAILED
        } else {
          null
        },
      streamNamespace = streamNamespace,
      metadata = metadata,
    )
}
