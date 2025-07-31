/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.airbyte.featureflag.DestinationTimeoutEnabled
import io.airbyte.featureflag.DestinationTimeoutSeconds
import io.airbyte.featureflag.FailSyncOnInvalidChecksum
import io.airbyte.featureflag.FieldSelectionEnabled
import io.airbyte.featureflag.LogConnectorMessages
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.featureflag.PrintLongRecordPks
import io.airbyte.featureflag.RemoveValidationLimit
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.featureflag.ShouldFailSyncIfHeartbeatFailure
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.featureflag.WorkloadHeartbeatRate
import io.airbyte.featureflag.WorkloadHeartbeatTimeout
import io.airbyte.workers.models.ReplicationFeatureFlags
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Instant
import java.util.function.Supplier

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  @Named("workspaceRoot")
  fun workspaceRoot(
    @Value("\${airbyte.workspace.root}") workspaceRoot: String,
  ): Path = Path.of(workspaceRoot)

  @Singleton
  @Named("currentSecondsSupplier")
  fun currentSecondsSupplier(): Supplier<Long> = Supplier { Instant.now().epochSecond }

  @Singleton
  @Named("replicationFeatureFlags")
  fun replicationFeatureFlags(): ReplicationFeatureFlags {
    val featureFlags =
      listOf(
        DestinationTimeoutEnabled,
        DestinationTimeoutSeconds,
        FailSyncOnInvalidChecksum,
        FieldSelectionEnabled,
        LogConnectorMessages,
        LogStateMsgs,
        PrintLongRecordPks,
        RemoveValidationLimit,
        ReplicationBufferOverride,
        ShouldFailSyncIfHeartbeatFailure,
        ShouldFailSyncOnDestinationTimeout,
        WorkloadHeartbeatRate,
        WorkloadHeartbeatTimeout,
      )
    return ReplicationFeatureFlags(featureFlags)
  }
}
