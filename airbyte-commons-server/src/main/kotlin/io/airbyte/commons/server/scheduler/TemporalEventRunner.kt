/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import io.opentelemetry.instrumentation.annotations.WithSpan
import java.util.UUID

/**
 * Temporal event client. For triggering events on connections.
 */
class TemporalEventRunner(
  private val temporalClient: TemporalClient,
) : EventRunner {
  @WithSpan
  override fun createConnectionManagerWorkflow(connectionId: UUID) {
    temporalClient.submitConnectionUpdaterAsync(connectionId)
  }

  @WithSpan
  override fun startNewManualSync(connectionId: UUID): ManualOperationResult = temporalClient.startNewManualSync(connectionId)

  @WithSpan
  override fun startNewCancellation(connectionId: UUID): ManualOperationResult = temporalClient.startNewCancellation(connectionId)

  @WithSpan
  override fun resetConnection(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ): ManualOperationResult = temporalClient.resetConnection(connectionId, streamsToReset)

  @WithSpan
  override fun refreshConnectionAsync(
    connectionId: UUID,
    streamsToRefresh: List<StreamDescriptor>,
    refreshType: RefreshStream.RefreshType,
  ) {
    temporalClient.refreshConnectionAsync(connectionId, streamsToRefresh, refreshType)
  }

  @WithSpan
  override fun resetConnectionAsync(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ) {
    temporalClient.resetConnectionAsync(connectionId, streamsToReset)
  }

  @WithSpan
  override fun forceDeleteConnection(connectionId: UUID) {
    temporalClient.forceDeleteWorkflow(connectionId)
  }

  @WithSpan
  override fun migrateSyncIfNeeded(connectionIds: Set<UUID>) {
    temporalClient.migrateSyncIfNeeded(connectionIds)
  }

  @WithSpan
  override fun update(connectionId: UUID) {
    temporalClient.update(connectionId)
  }
}
