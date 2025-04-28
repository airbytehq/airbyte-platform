/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.config.StandardSyncInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import jakarta.inject.Singleton

@ActivityInterface
interface GenerateReplicationActivityInputActivity {
  @ActivityMethod
  fun generate(
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    taskQueue: String,
    refreshSchemaOutput: RefreshSchemaActivityOutput,
    signalInput: String?,
  ): ReplicationActivityInput
}

@Singleton
class GenerateReplicationActivityInputActivityImpl : GenerateReplicationActivityInputActivity {
  override fun generate(
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    taskQueue: String,
    refreshSchemaOutput: RefreshSchemaActivityOutput,
    signalInput: String?,
  ): ReplicationActivityInput =
    ReplicationActivityInput(
      syncInput.sourceId,
      syncInput.destinationId,
      syncInput.sourceConfiguration,
      syncInput.destinationConfiguration,
      jobRunConfig,
      sourceLauncherConfig,
      destinationLauncherConfig,
      syncInput.syncResourceRequirements,
      syncInput.workspaceId,
      syncInput.connectionId,
      taskQueue,
      syncInput.isReset,
      syncInput.namespaceDefinition,
      syncInput.namespaceFormat,
      syncInput.prefix,
      refreshSchemaOutput,
      syncInput.connectionContext,
      signalInput,
      syncInput.networkSecurityTokens,
      syncInput.includesFiles,
      syncInput.omitFileTransferEnvVar,
    )
}
