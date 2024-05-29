package io.airbyte.workers.internal.bookkeeping.streamstatus

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.context.ReplicationContext
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
  private val metricClient: MetricClient,
  private val clock: Clock,
) {
  private val cache = HashMap<StreamStatusKey, StreamStatusRead>()
  private lateinit var ctx: ReplicationContext

  /**
   * The replication context (workspace, job, attempt, connection id, etc.) is not known at injection time for docker,
   * so we must have a goofy init function and handle the cases where it is not initialized.
   */
  fun init(ctx: ReplicationContext) {
    if (this::ctx.isInitialized) {
      logger.error { "Replication context has already been initialized." }
      return
    }

    this.ctx = ctx
  }

  fun put(
    key: StreamStatusKey,
    runState: ApiEnum,
  ) {
    logger.debug { "Stream Status Update Received: ${key.toDisplayName()} - $runState" }

    if (shouldAbortBecauseNotInitialized()) {
      return
    }

    val value = cache[key]

    if (value == null) {
      logger.debug { "Creating status: ${key.toDisplayName()} - $runState" }
      val req = buildCreateReq(key.streamNamespace, key.streamName, runState)

      val resp = airbyteApiClient.streamStatusesApi.createStreamStatus(req)
      cache[key] = resp
    } else if (value.runState != runState) {
      logger.debug { "Updating status: ${key.toDisplayName()} - $runState" }
      val req = buildUpdateReq(value.id, key.streamNamespace, key.streamName, runState)

      val resp = airbyteApiClient.streamStatusesApi.updateStreamStatus(req)
      cache[key] = resp
    } else {
      logger.debug { "Stream ${key.toDisplayName()} is already set to $runState. Ignoring..." }
    }
  }

  @VisibleForTesting
  fun buildCreateReq(
    streamNamespace: String?,
    streamName: String,
    runState: ApiEnum,
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
    )

  @VisibleForTesting
  fun buildUpdateReq(
    id: UUID,
    streamNamespace: String?,
    streamName: String,
    runState: ApiEnum,
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
    )

  private fun shouldAbortBecauseNotInitialized(): Boolean {
    if (this::ctx.isInitialized) {
      return false
    }

    // We don't want to throw exceptions that could affect sync progress if this isn't initialized,
    // but we do not expect / want this to happen, so we record a metric for visibility.
    logger.error { "Replication context has not been initialized." }
    metricClient.count(
      OssMetricsRegistry.REPLICATION_CONTEXT_NOT_INITIALIZED_ERROR,
      1,
      MetricAttribute(MetricTags.EMITTING_CLASS, this.javaClass.simpleName),
    )
    return true
  }
}
