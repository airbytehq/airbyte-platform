/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.workers.context.ReplicationFeatureFlags;

/**
 * Read features flags we need to consider during a sync.
 */
public class ReplicationFeatureFlagReader {

  /**
   * Read Feature flags we need to consider during a sync.
   *
   * @param syncInput the input of the sync.
   * @return The flags.
   */
  public ReplicationFeatureFlags readReplicationFeatureFlags(final StandardSyncInput syncInput) {
    return new ReplicationFeatureFlags(
        ReplicationFeatureFlagReader.shouldCommitStateAsap(syncInput),
        ReplicationFeatureFlagReader.shouldCommitStatsAsap(syncInput));
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

}
