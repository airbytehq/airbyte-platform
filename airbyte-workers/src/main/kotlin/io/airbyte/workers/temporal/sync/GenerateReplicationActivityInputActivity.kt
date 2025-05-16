/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.commons.helper.DockerImageName
import io.airbyte.config.StandardSyncInput
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceType
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.ReplicationFeatureFlags
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
      heartbeatMaxSecondsBetweenMessages: Long? = DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES,
      supportsRefreshes: Boolean = false,
      sourceIPCOptions: JsonNode? = null,
      destinationIPCOptions: JsonNode? = null,
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
      sourceIPCOptions = sourceIPCOptions,
      destinationIPCOptions = destinationIPCOptions,
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
  ): ReplicationActivityInput {
    val heartbeatSeconds = retrieveHeartbeatMaxSeconds(sourceDefinitionId = syncInput.connectionContext.sourceDefinitionId)

    val sourceDef =
      fetchActorDefinitionVersion(
        definitionId = syncInput.connectionContext.sourceDefinitionId,
        actorType = ActorType.SOURCE,
        dockerImageTag = DockerImageName.extractTag(sourceLauncherConfig.dockerImage),
      )
    val destDef =
      fetchActorDefinitionVersion(
        definitionId = syncInput.connectionContext.destinationDefinitionId,
        actorType = ActorType.DESTINATION,
        dockerImageTag = DockerImageName.extractTag(destinationLauncherConfig.dockerImage),
      )

    return toReplicationActivityInput(
      syncInput = syncInput,
      jobRunConfig = jobRunConfig,
      sourceLauncherConfig = sourceLauncherConfig,
      destinationLauncherConfig = destinationLauncherConfig,
      taskQueue = taskQueue,
      refreshSchemaOutput = refreshSchemaOutput,
      signalInput = signalInput,
      featureFlags = resolveFeatureFlags(syncInput = syncInput),
      heartbeatMaxSecondsBetweenMessages = heartbeatSeconds,
      supportsRefreshes = destDef?.supportRefreshes ?: false,
      sourceIPCOptions = sourceDef?.connectorIPCOptions,
      destinationIPCOptions = destDef?.connectorIPCOptions,
    )
  }

  private fun retrieveHeartbeatMaxSeconds(sourceDefinitionId: UUID?): Long =
    runCatching {
      sourceDefinitionId
        ?.let { SourceDefinitionIdRequestBody(it) }
        ?.let { airbyteApiClient.sourceDefinitionApi.getSourceDefinition(it).maxSecondsBetweenMessages }
        ?: DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES
    }.onFailure { e ->
      logger.warn(e) { "Failed to fetch heartbeat config—falling back to $DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES s" }
    }.getOrDefault(DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES)

  private fun fetchActorDefinitionVersion(
    definitionId: UUID?,
    actorType: ActorType,
    dockerImageTag: String,
  ) = definitionId
    ?.let { id ->
      runCatching {
        airbyteApiClient.actorDefinitionVersionApi.resolveActorDefinitionVersionByTag(
          ResolveActorDefinitionVersionRequestBody(
            actorDefinitionId = id,
            actorType = actorType,
            dockerImageTag = dockerImageTag,
          ),
        )
      }.onFailure { e ->
        logger.warn(e) { "Could not resolve $actorType definition for ID=$id, tag=$dockerImageTag" }
      }.getOrNull()
    }

  private fun resolveFeatureFlags(syncInput: StandardSyncInput): Map<String, Any> {
    val contexts =
      listOfNotNull(
        syncInput.workspaceId?.let(::Workspace),
        syncInput.connectionId?.let(::Connection),
        syncInput.sourceId?.let(::Source),
        syncInput.destinationId?.let(::Destination),
        syncInput.syncResourceRequirements
          ?.configKey
          ?.subType
          ?.let(::SourceType),
      )
    val multiCtx = Multi(contexts)
    return replicationFeatureFlags.featureFlags.associate { flag ->
      flag.key to (featureFlagClient.variation(flag, multiCtx)!!)
    }
  }
}
