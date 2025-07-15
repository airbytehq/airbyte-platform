/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import java.util.UUID

/**
 * Client for triggering events on connections.
 */
interface EventRunner {
  fun createConnectionManagerWorkflow(connectionId: UUID)

  fun startNewManualSync(connectionId: UUID): ManualOperationResult

  fun startNewCancellation(connectionId: UUID): ManualOperationResult

  fun resetConnection(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ): ManualOperationResult

  fun refreshConnectionAsync(
    connectionId: UUID,
    streamsToRefresh: List<StreamDescriptor>,
    refreshType: RefreshStream.RefreshType,
  )

  fun resetConnectionAsync(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  )

  fun forceDeleteConnection(connectionId: UUID)

  // TODO: Delete
  @Deprecated("")
  fun migrateSyncIfNeeded(connectionIds: Set<UUID>)

  fun update(connectionId: UUID)
}
