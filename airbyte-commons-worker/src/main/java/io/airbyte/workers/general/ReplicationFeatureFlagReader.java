/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.DestinationTimeoutEnabled;
import io.airbyte.featureflag.FailSyncOnInvalidChecksum;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.LogConnectorMessages;
import io.airbyte.featureflag.LogStateMsgs;
import io.airbyte.featureflag.UseFileTransferMode;
import io.airbyte.featureflag.WorkloadHeartbeatRate;
import io.airbyte.featureflag.WorkloadHeartbeatTimeout;
import io.airbyte.workers.context.ReplicationFeatureFlags;

/**
 * Read features flags we need to consider during a sync.
 */
public class ReplicationFeatureFlagReader {

  private final FeatureFlagClient featureFlagClient;
  private final Context flagContext;

  public ReplicationFeatureFlagReader(final FeatureFlagClient featureFlagClient, final Context flagContext) {
    this.featureFlagClient = featureFlagClient;
    this.flagContext = flagContext;
  }

  /**
   * Read Feature flags we need to consider during a sync.
   *
   * @return The flags.
   */
  public ReplicationFeatureFlags readReplicationFeatureFlags() {
    return new ReplicationFeatureFlags(isDestinationTimeoutEnabled(), getWorkloadHeartbeatRate(), getWorkloadHeartbeatTimeout(),
        failOnInvalidChecksum(), logStateMessages(), logConnectorMessages(), useFileTransfer());
  }

  private int getWorkloadHeartbeatRate() {
    return featureFlagClient.intVariation(WorkloadHeartbeatRate.INSTANCE, flagContext);
  }

  private int getWorkloadHeartbeatTimeout() {
    return featureFlagClient.intVariation(WorkloadHeartbeatTimeout.INSTANCE, flagContext);
  }

  private boolean isDestinationTimeoutEnabled() {
    return featureFlagClient.boolVariation(DestinationTimeoutEnabled.INSTANCE, flagContext);
  }

  private boolean failOnInvalidChecksum() {
    return featureFlagClient.boolVariation(FailSyncOnInvalidChecksum.INSTANCE, flagContext);
  }

  private boolean logStateMessages() {
    return featureFlagClient.boolVariation(LogStateMsgs.INSTANCE, flagContext);
  }

  private boolean logConnectorMessages() {
    return featureFlagClient.boolVariation(LogConnectorMessages.INSTANCE, flagContext);
  }

  /**
   * This is a very temporary flag to test the file transfer mode. It will be removed once we have a
   * good way to read the need to use file transfer mode from the actor config. This flag is anonymous
   * because for our test we don't have to scope it. It is also use in the
   * {@link ReplicationPodFactory} with an anonymous scope.
   */
  private boolean useFileTransfer() {
    return featureFlagClient.boolVariation(UseFileTransferMode.INSTANCE, new Connection(ANONYMOUS));
  }

}
