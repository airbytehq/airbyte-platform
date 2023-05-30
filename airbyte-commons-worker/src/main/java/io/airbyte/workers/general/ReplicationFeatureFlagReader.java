/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HandleStreamStatus;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;

/**
 * Read features flags we need to consider during a sync.
 */
public class ReplicationFeatureFlagReader {

  private final FeatureFlagClient featureFlagClient;

  public ReplicationFeatureFlagReader(final FeatureFlagClient featureFlagClient) {
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Read Feature flags we need to consider during a sync.
   *
   * @param replicationContext the context of the sync.
   * @param syncInput the input of the sync.
   * @return The flags.
   */
  public ReplicationFeatureFlags readReplicationFeatureFlags(final ReplicationContext replicationContext, final StandardSyncInput syncInput) {
    return new ReplicationFeatureFlags(
        ReplicationFeatureFlagReader.shouldCommitStateAsap(syncInput),
        ReplicationFeatureFlagReader.shouldCommitStatsAsap(syncInput),
        shouldHandleStreamStatus(replicationContext));
  }

  /**
   * Helper function to read the shouldCommitStateAsap feature flag.
   */
  static boolean shouldCommitStateAsap(final StandardSyncInput syncInput) {
    return syncInput.getCommitStateAsap() != null && syncInput.getCommitStateAsap();
  }

  /**
   * Helper function to read the shouldCommitStatsAsap feature flag.
   */
  static boolean shouldCommitStatsAsap(final StandardSyncInput syncInput) {
    // For consistency, we should only be committing stats early if we are committing states early.
    // Otherwise, we are risking stats discrepancy as we are committing stats for states that haven't
    // been persisted yet.
    return shouldCommitStateAsap(syncInput) && syncInput.getCommitStatsAsap() != null && syncInput.getCommitStatsAsap();
  }

  /**
   * Helper function to read the status of the {@link HandleStreamStatus} feature flag once at the
   * start of the replication exection.
   *
   * @param replicationContext The {@link ReplicationContext} of the replication.
   * @return The result of checking the status of the {@link HandleStreamStatus} feature flag.
   */
  private boolean shouldHandleStreamStatus(final ReplicationContext replicationContext) {
    return featureFlagClient.boolVariation(HandleStreamStatus.INSTANCE, new Workspace(replicationContext.workspaceId()));
  }

}
