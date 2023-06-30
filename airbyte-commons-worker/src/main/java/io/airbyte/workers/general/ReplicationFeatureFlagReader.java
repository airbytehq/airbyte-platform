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
    return new ReplicationFeatureFlags();
  }

}
