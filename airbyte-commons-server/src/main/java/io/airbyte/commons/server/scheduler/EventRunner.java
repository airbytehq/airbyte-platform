/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import io.airbyte.commons.temporal.ManualOperationResult;
import io.airbyte.config.RefreshStream.RefreshType;
import io.airbyte.config.StreamDescriptor;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Client for triggering events on connections.
 */
public interface EventRunner {

  void createConnectionManagerWorkflow(final UUID connectionId);

  ManualOperationResult startNewManualSync(final UUID connectionId);

  ManualOperationResult startNewCancellation(final UUID connectionId);

  ManualOperationResult resetConnection(final UUID connectionId, final List<StreamDescriptor> streamsToReset);

  void refreshConnectionAsync(UUID connectionId, List<StreamDescriptor> streamsToRefresh, RefreshType refreshType);

  void resetConnectionAsync(UUID connectionId, List<StreamDescriptor> streamsToReset);

  void forceDeleteConnection(final UUID connectionId);

  // TODO: Delete
  @Deprecated(forRemoval = true)
  void migrateSyncIfNeeded(final Set<UUID> connectionIds);

  void update(final UUID connectionId);

}
