/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import java.util.UUID

/**
 * Temporal event client. For triggering events on connections.
 */
class TemporalEventRunner(
  private val temporalClient: TemporalClient,
) : EventRunner {
  @Trace
  override fun createConnectionManagerWorkflow(connectionId: UUID) {
    temporalClient.submitConnectionUpdaterAsync(connectionId)
  }

  @Trace
  override fun startNewManualSync(connectionId: UUID): ManualOperationResult = temporalClient.startNewManualSync(connectionId)

  @Trace
  override fun startNewCancellation(connectionId: UUID): ManualOperationResult = temporalClient.startNewCancellation(connectionId)

  @Trace
  override fun resetConnection(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ): ManualOperationResult = temporalClient.resetConnection(connectionId, streamsToReset)

  @Trace
  override fun refreshConnectionAsync(
    connectionId: UUID,
    streamsToRefresh: List<StreamDescriptor>,
    refreshType: RefreshStream.RefreshType,
  ) {
    temporalClient.refreshConnectionAsync(connectionId, streamsToRefresh, refreshType)
  }

  @Trace
  override fun resetConnectionAsync(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ) {
    temporalClient.resetConnectionAsync(connectionId, streamsToReset)
  }

  @Trace
  override fun forceDeleteConnection(connectionId: UUID) {
    temporalClient.forceDeleteWorkflow(connectionId)
  }

  @Trace
  override fun migrateSyncIfNeeded(connectionIds: Set<UUID>) {
    temporalClient.migrateSyncIfNeeded(connectionIds)
  }

  @Trace
  override fun update(connectionId: UUID) {
    temporalClient.update(connectionId)
  }
}
