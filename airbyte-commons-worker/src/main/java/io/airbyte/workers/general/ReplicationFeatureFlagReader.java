/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.context.ReplicationFeatureFlags;

/**
 * Read features flags we need to consider during a sync.
 */
public class ReplicationFeatureFlagReader {

  /**
   * Read Feature flags we need to consider during a sync.
   *
   * @param replicationInput the input of the sync.
   * @return The flags.
   */
  public ReplicationFeatureFlags readReplicationFeatureFlags(final ReplicationInput replicationInput) {
    return new ReplicationFeatureFlags();
  }

}
