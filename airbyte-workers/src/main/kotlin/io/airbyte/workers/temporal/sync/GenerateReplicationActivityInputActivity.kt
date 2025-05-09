/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.commons.helper.DockerImageName
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
import java.util.UUID
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

internal val DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES = TimeUnit.HOURS.toSeconds(24)

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
      supportsRefreshes: Boolean = false,
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
      supportsRefreshes = supportsRefreshes,
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
      heartbeatMaxSecondsBetweenMessages =
        retrieveHeartbeatMaxSecondsBetweenMessages(
          sourceDefinitionId = syncInput.connectionContext.sourceDefinitionId,
          airbyteApiClient = airbyteApiClient,
        ),
      supportsRefreshes =
        supportsRefreshes(
          destinationDefinitionId = syncInput.connectionContext.destinationDefinitionId,
          destinationLauncherConfig = destinationLauncherConfig,
          airbyteApiClient = airbyteApiClient,
        ),
    )

  private fun resolveFeatureFlags(syncInput: StandardSyncInput): Map<String, Any> {
    val context = getFeatureFlagContext(syncInput = syncInput)
    return replicationFeatureFlags.featureFlags.associate { flag -> flag.key to featureFlagClient.variation(flag, context)!! }
  }

  private fun retrieveHeartbeatMaxSecondsBetweenMessages(
    sourceDefinitionId: UUID?,
    airbyteApiClient: AirbyteApiClient,
  ): Long? =
    try {
      sourceDefinitionId?.let { id ->
        airbyteApiClient.sourceDefinitionApi.getSourceDefinition(SourceDefinitionIdRequestBody(id)).maxSecondsBetweenMessages
      } ?: DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES
    } catch (e: Exception) {
      logger.warn(e) {
        "An error occurred while fetching the max seconds between messages for this source. We are using a default of $DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES hours"
      }
      DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES
    }

  private fun supportsRefreshes(
    destinationDefinitionId: UUID?,
    destinationLauncherConfig: IntegrationLauncherConfig,
    airbyteApiClient: AirbyteApiClient,
  ): Boolean =
    destinationDefinitionId?.let { id ->
      try {
        airbyteApiClient.actorDefinitionVersionApi
          .resolveActorDefinitionVersionByTag(
            ResolveActorDefinitionVersionRequestBody(
              actorDefinitionId = id,
              actorType = ActorType.DESTINATION,
              dockerImageTag = DockerImageName.extractTag(destinationLauncherConfig.dockerImage),
            ),
          ).supportRefreshes
      } catch (e: Exception) {
        logger.warn(e) {
          "An error occurred while fetching the actor definition associated with destination definition ID $id.  Supports refreshes will be set to false."
        }
        false
      }
    } ?: false

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
