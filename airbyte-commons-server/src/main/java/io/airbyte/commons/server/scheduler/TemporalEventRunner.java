/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.ManualOperationResult;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.config.RefreshStream.RefreshType;
import io.airbyte.config.StreamDescriptor;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Temporal event client. For triggering events on connections.
 */
public class TemporalEventRunner implements EventRunner {

  private final TemporalClient temporalClient;

  public TemporalEventRunner(TemporalClient temporalClient) {
    this.temporalClient = temporalClient;
  }

  @Override
  @Trace
  public void createConnectionManagerWorkflow(final UUID connectionId) {
    temporalClient.submitConnectionUpdaterAsync(connectionId);
  }

  @Override
  @Trace
  public ManualOperationResult startNewManualSync(final UUID connectionId) {
    return temporalClient.startNewManualSync(connectionId);
  }

  @Override
  @Trace
  public ManualOperationResult startNewCancellation(final UUID connectionId) {
    return temporalClient.startNewCancellation(connectionId);
  }

  @Override
  @Trace
  public ManualOperationResult resetConnection(final UUID connectionId,
                                               final List<StreamDescriptor> streamsToReset) {
    return temporalClient.resetConnection(connectionId, streamsToReset);
  }

  @Override
  @Trace
  public void refreshConnectionAsync(final UUID connectionId,
                                     final List<StreamDescriptor> streamsToRefresh,
                                     final RefreshType refreshType) {
    temporalClient.refreshConnectionAsync(connectionId, streamsToRefresh, refreshType);
  }

  @Override
  @Trace
  public void resetConnectionAsync(final UUID connectionId,
                                   final List<StreamDescriptor> streamsToReset) {
    temporalClient.resetConnectionAsync(connectionId, streamsToReset);
  }

  @Override
  @Trace
  public void forceDeleteConnection(final UUID connectionId) {
    temporalClient.forceDeleteWorkflow(connectionId);
  }

  @Override
  @Trace
  public void migrateSyncIfNeeded(final Set<UUID> connectionIds) {
    temporalClient.migrateSyncIfNeeded(connectionIds);
  }

  @Override
  @Trace
  public void update(final UUID connectionId) {
    temporalClient.update(connectionId);
  }

}
