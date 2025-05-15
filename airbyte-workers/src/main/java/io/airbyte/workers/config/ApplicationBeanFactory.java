/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.featureflag.DestinationTimeoutEnabled;
import io.airbyte.featureflag.DestinationTimeoutSeconds;
import io.airbyte.featureflag.FailSyncOnInvalidChecksum;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.Flag;
import io.airbyte.featureflag.LogConnectorMessages;
import io.airbyte.featureflag.LogStateMsgs;
import io.airbyte.featureflag.PrintLongRecordPks;
import io.airbyte.featureflag.RemoveValidationLimit;
import io.airbyte.featureflag.ReplicationBufferOverride;
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout;
import io.airbyte.featureflag.WorkloadHeartbeatRate;
import io.airbyte.featureflag.WorkloadHeartbeatTimeout;
import io.airbyte.workers.models.ReplicationFeatureFlags;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ApplicationBeanFactory {

  @Singleton
  @Named("workspaceRoot")
  public Path workspaceRoot(@Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

  @Singleton
  @Named("currentSecondsSupplier")
  public Supplier<Long> currentSecondsSupplier() {
    return () -> Instant.now().getEpochSecond();
  }

  @Singleton
  @Named("replicationFeatureFlags")
  public ReplicationFeatureFlags replicationFeatureFlags() {
    final List<Flag<?>> featureFlags = List.of(
        DestinationTimeoutEnabled.INSTANCE,
        DestinationTimeoutSeconds.INSTANCE,
        FailSyncOnInvalidChecksum.INSTANCE,
        FieldSelectionEnabled.INSTANCE,
        LogConnectorMessages.INSTANCE,
        LogStateMsgs.INSTANCE,
        PrintLongRecordPks.INSTANCE,
        RemoveValidationLimit.INSTANCE,
        ReplicationBufferOverride.INSTANCE,
        ShouldFailSyncOnDestinationTimeout.INSTANCE,
        WorkloadHeartbeatRate.INSTANCE,
        WorkloadHeartbeatTimeout.INSTANCE);
    return new ReplicationFeatureFlags(featureFlags);
  }

}
