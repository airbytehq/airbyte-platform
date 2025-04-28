/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.config.StandardSyncInput
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceType
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.temporal.sync.GenerateReplicationActivityInputActivity.Companion.toReplicationActivityInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

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

  companion object {
    @JvmStatic
    fun toReplicationActivityInput(
      syncInput: StandardSyncInput,
      jobRunConfig: JobRunConfig,
      sourceLauncherConfig: IntegrationLauncherConfig,
      destinationLauncherConfig: IntegrationLauncherConfig,
      taskQueue: String,
      refreshSchemaOutput: RefreshSchemaActivityOutput,
      signalInput: String?,
      featureFlags: Map<String, Any> = emptyMap(),
      heartbeatMaxSecondsBetweenMessages: Long? = TimeUnit.HOURS.toSeconds(24),
    ) = ReplicationActivityInput(
      sourceId = syncInput.sourceId,
      destinationId = syncInput.destinationId,
      sourceConfiguration = syncInput.sourceConfiguration,
      destinationConfiguration = syncInput.destinationConfiguration,
      jobRunConfig = jobRunConfig,
      sourceLauncherConfig = sourceLauncherConfig,
      destinationLauncherConfig = destinationLauncherConfig,
      syncResourceRequirements = syncInput.syncResourceRequirements,
      workspaceId = syncInput.workspaceId,
      connectionId = syncInput.connectionId,
      taskQueue = taskQueue,
      isReset = syncInput.isReset,
      namespaceDefinition = syncInput.namespaceDefinition,
      namespaceFormat = syncInput.namespaceFormat,
      prefix = syncInput.prefix,
      schemaRefreshOutput = refreshSchemaOutput,
      connectionContext = syncInput.connectionContext,
      signalInput = signalInput,
      networkSecurityTokens = syncInput.networkSecurityTokens,
      includesFiles = syncInput.includesFiles,
      omitFileTransferEnvVar = syncInput.omitFileTransferEnvVar,
      featureFlags = featureFlags,
      heartbeatMaxSecondsBetweenMessages = heartbeatMaxSecondsBetweenMessages,
    )
  }
}

@Singleton
class GenerateReplicationActivityInputActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val replicationFeatureFlags: ReplicationFeatureFlags,
) : GenerateReplicationActivityInputActivity {
  override fun generate(
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    taskQueue: String,
    refreshSchemaOutput: RefreshSchemaActivityOutput,
    signalInput: String?,
  ): ReplicationActivityInput =
    toReplicationActivityInput(
      syncInput = syncInput,
      jobRunConfig = jobRunConfig,
      sourceLauncherConfig = sourceLauncherConfig,
      destinationLauncherConfig = destinationLauncherConfig,
      taskQueue = taskQueue,
      refreshSchemaOutput = refreshSchemaOutput,
      signalInput = signalInput,
      featureFlags = resolveFeatureFlags(syncInput = syncInput),
      heartbeatMaxSecondsBetweenMessages = retrieveHeartbeatMaxSecondsBetweenMessages(syncInput = syncInput, airbyteApiClient = airbyteApiClient),
    )

  private fun resolveFeatureFlags(syncInput: StandardSyncInput): Map<String, Any> {
    val context = getFeatureFlagContext(syncInput = syncInput)
    return replicationFeatureFlags.featureFlags.associate { flag -> flag.key to featureFlagClient.variation(flag, context)!! }
  }

  private fun retrieveHeartbeatMaxSecondsBetweenMessages(
    syncInput: StandardSyncInput,
    airbyteApiClient: AirbyteApiClient,
  ): Long? =
    try {
      val sourceDefinitionId = airbyteApiClient.sourceApi.getSource(SourceIdRequestBody(syncInput.sourceId)).sourceDefinitionId
      airbyteApiClient.sourceDefinitionApi.getSourceDefinition(SourceDefinitionIdRequestBody(sourceDefinitionId)).maxSecondsBetweenMessages
    } catch (e: Exception) {
      logger.warn(e) { "An error occurred while fetch the max seconds between messages for this source. We are using a default of 24 hours" }
      TimeUnit.HOURS.toSeconds(24)
    }

  private fun getFeatureFlagContext(syncInput: StandardSyncInput): Context {
    val contexts: MutableList<Context> = mutableListOf()
    if (syncInput.workspaceId != null) {
      contexts.add(Workspace(syncInput.workspaceId))
    }
    if (syncInput.connectionId != null) {
      contexts.add(Connection(syncInput.connectionId))
    }
    if (syncInput.sourceId != null) {
      contexts.add(Source(syncInput.sourceId))
    }
    if (syncInput.destinationId != null) {
      contexts.add(Destination(syncInput.destinationId))
    }
    if (syncInput.syncResourceRequirements != null &&
      syncInput.syncResourceRequirements
        .configKey != null &&
      syncInput.syncResourceRequirements.configKey.subType != null
    ) {
      contexts.add(SourceType(syncInput.syncResourceRequirements.configKey.subType))
    }
    return Multi(contexts)
  }
}

data class ReplicationFeatureFlags(
  val featureFlags: List<Flag<*>>,
)
