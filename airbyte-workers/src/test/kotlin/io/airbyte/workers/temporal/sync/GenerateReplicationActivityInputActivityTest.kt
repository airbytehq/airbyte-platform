/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.generated.SourceDefinitionApi
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionResponse
import io.airbyte.api.client.model.generated.SourceDefinitionRead
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.SyncResourceRequirementsKey
import io.airbyte.featureflag.DestinationTimeoutEnabled
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationFeatureFlags
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
import java.util.UUID

internal class GenerateReplicationActivityInputActivityTest {
  @ParameterizedTest
  @MethodSource("signalInputProvider")
  @Suppress("UNCHECKED_CAST")
  fun testGeneratingReplicationActivityInput(signalInput: String?) {
    val destinationDefinitionUUID = UUID.randomUUID()
    val destinationSupportRefreshes = true
    val sourceDefinitionUUID = UUID.randomUUID()
    val heartbeatMaxSecondsBetweenMessages = 1000L
    val resolveActorDefinitionVersionResponse =
      mockk<ResolveActorDefinitionVersionResponse> {
        every { supportRefreshes } returns destinationSupportRefreshes
        every { connectorIPCOptions } returns null
      }
    val actorDefinitionVersionApiClient =
      mockk<ActorDefinitionVersionApi> {
        every { resolveActorDefinitionVersionByTag(any()) } returns resolveActorDefinitionVersionResponse
      }
    val sourceDefinitionRead =
      mockk<SourceDefinitionRead> {
        every { maxSecondsBetweenMessages } returns heartbeatMaxSecondsBetweenMessages
      }
    val sourceDefinitionApiClient =
      mockk<SourceDefinitionApi> {
        every { getSourceDefinition(any()) } returns sourceDefinitionRead
      }
    val airbyteApiClient =
      mockk<AirbyteApiClient> {
        every { actorDefinitionVersionApi } returns actorDefinitionVersionApiClient
        every { sourceDefinitionApi } returns sourceDefinitionApiClient
      }
    val replicationFeatureFlags = ReplicationFeatureFlags(listOf(DestinationTimeoutEnabled as Flag<Any>))
    val resolvedFeatureFlags = mapOf(DestinationTimeoutEnabled.key to true)
    val featureFlagClient = TestClient(resolvedFeatureFlags)

    val connectionCtx =
      mockk<ConnectionContext> {
        every { sourceDefinitionId } returns sourceDefinitionUUID
        every { destinationDefinitionId } returns destinationDefinitionUUID
      }
    val connectionUUID = UUID.randomUUID()
    val destinationConfigurationJson = Jsons.jsonNode("{}")
    val image = "airbyte/test:1.2"
    val destinationUUID = UUID.randomUUID()
    val namespaceDefinitionType = JobSyncConfig.NamespaceDefinitionType.SOURCE
    val namespaceFormatString = "test_format"
    val prefixString = "test_prefix"
    val resourceRequirementsConfigKey =
      mockk<SyncResourceRequirementsKey> {
        every { subType } returns "test_sub_type"
      }
    val securityTokens = listOf("token1", "token2")
    val shouldOmitFieTransferEnvVar = true
    val shouldIncludeFiles = true
    val shouldReset = true
    val sourceConfigurationJson = Jsons.jsonNode("{}")
    val sourceUUID = UUID.randomUUID()
    val syncResourceReqs =
      mockk<SyncResourceRequirements> {
        every { configKey } returns resourceRequirementsConfigKey
      }
    val workspaceUUID = UUID.randomUUID()

    val destinationLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val jobRunConfig = mockk<JobRunConfig>()
    val refreshSchemaOutput = mockk<RefreshSchemaActivityOutput>()
    val sourceLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { connectionContext } returns connectionCtx
        every { connectionId } returns connectionUUID
        every { destinationConfiguration } returns destinationConfigurationJson
        every { destinationId } returns destinationUUID
        every { includesFiles } returns shouldIncludeFiles
        every { isReset } returns shouldReset
        every { namespaceDefinition } returns namespaceDefinitionType
        every { networkSecurityTokens } returns securityTokens
        every { prefix } returns prefixString
        every { namespaceFormat } returns namespaceFormatString
        every { omitFileTransferEnvVar } returns shouldOmitFieTransferEnvVar
        every { sourceConfiguration } returns sourceConfigurationJson
        every { sourceId } returns sourceUUID
        every { syncResourceRequirements } returns syncResourceReqs
        every { workspaceId } returns workspaceUUID
      }
    val taskQueue = "test-task-queue"
    val activity =
      GenerateReplicationActivityInputActivityImpl(
        airbyteApiClient = airbyteApiClient,
        featureFlagClient = featureFlagClient,
        replicationFeatureFlags = replicationFeatureFlags,
      )

    val input =
      activity.generate(
        syncInput = syncInput,
        jobRunConfig = jobRunConfig,
        sourceLauncherConfig = sourceLauncherConfig,
        destinationLauncherConfig = destinationLauncherConfig,
        taskQueue = taskQueue,
        refreshSchemaOutput = refreshSchemaOutput,
        signalInput = signalInput,
      )

    assertEquals(connectionCtx, input.connectionContext)
    assertEquals(connectionUUID, input.connectionId)
    assertEquals(destinationConfigurationJson, input.destinationConfiguration)
    assertEquals(destinationDefinitionUUID, input.connectionContext?.destinationDefinitionId)
    assertEquals(destinationUUID, input.destinationId)
    assertEquals(destinationLauncherConfig, input.destinationLauncherConfig)
    assertEquals(destinationSupportRefreshes, input.supportsRefreshes)
    assertEquals(heartbeatMaxSecondsBetweenMessages, input.heartbeatMaxSecondsBetweenMessages)
    assertEquals(jobRunConfig, input.jobRunConfig)
    assertEquals(namespaceDefinitionType, input.namespaceDefinition)
    assertEquals(namespaceFormatString, input.namespaceFormat)
    assertEquals(prefixString, input.prefix)
    assertEquals(refreshSchemaOutput, input.schemaRefreshOutput)
    assertEquals(resolvedFeatureFlags, input.featureFlags)
    assertEquals(securityTokens, input.networkSecurityTokens)
    assertEquals(shouldOmitFieTransferEnvVar, input.omitFileTransferEnvVar)
    assertEquals(shouldIncludeFiles, input.includesFiles)
    assertEquals(shouldReset, input.isReset)
    assertEquals(signalInput, input.signalInput)
    assertEquals(sourceConfigurationJson, input.sourceConfiguration)
    assertEquals(sourceDefinitionUUID, input.connectionContext?.sourceDefinitionId)
    assertEquals(sourceLauncherConfig, input.sourceLauncherConfig)
    assertEquals(sourceUUID, input.sourceId)
    assertEquals(syncResourceReqs, input.syncResourceRequirements)
    assertEquals(taskQueue, input.taskQueue)
    assertEquals(workspaceUUID, input.workspaceId)
  }

  @Test
  @Suppress("UNCHECKED_CAST")
  fun testGeneratingReplicationActivityInputWithActorDefinitionRetrievalFailure() {
    val destinationDefinitionUUID = UUID.randomUUID()
    val destinationSupportRefreshes = false
    val sourceDefinitionUUID = UUID.randomUUID()
    val heartbeatMaxSecondsBetweenMessages = 1000L
    val actorDefinitionVersionApiClient =
      mockk<ActorDefinitionVersionApi> {
        every { resolveActorDefinitionVersionByTag(any()) } throws IOException("Failed to retrieve actor definition.")
      }
    val sourceDefinitionRead =
      mockk<SourceDefinitionRead> {
        every { maxSecondsBetweenMessages } returns heartbeatMaxSecondsBetweenMessages
      }
    val sourceDefinitionApiClient =
      mockk<SourceDefinitionApi> {
        every { getSourceDefinition(any()) } returns sourceDefinitionRead
      }
    val airbyteApiClient =
      mockk<AirbyteApiClient> {
        every { actorDefinitionVersionApi } returns actorDefinitionVersionApiClient
        every { sourceDefinitionApi } returns sourceDefinitionApiClient
      }
    val replicationFeatureFlags = ReplicationFeatureFlags(listOf(DestinationTimeoutEnabled as Flag<Any>))
    val resolvedFeatureFlags = mapOf(DestinationTimeoutEnabled.key to true)
    val featureFlagClient = TestClient(resolvedFeatureFlags)

    val connectionCtx =
      mockk<ConnectionContext> {
        every { sourceDefinitionId } returns sourceDefinitionUUID
        every { destinationDefinitionId } returns destinationDefinitionUUID
      }
    val connectionUUID = UUID.randomUUID()
    val destinationConfigurationJson = Jsons.jsonNode("{}")
    val image = "airbyte/test:1.2"
    val destinationUUID = UUID.randomUUID()
    val namespaceDefinitionType = JobSyncConfig.NamespaceDefinitionType.SOURCE
    val namespaceFormatString = "test_format"
    val prefixString = "test_prefix"
    val resourceRequirementsConfigKey =
      mockk<SyncResourceRequirementsKey> {
        every { subType } returns "test_sub_type"
      }
    val securityTokens = listOf("token1", "token2")
    val shouldOmitFieTransferEnvVar = true
    val shouldIncludeFiles = true
    val shouldReset = true
    val signalInput = "{\"signal-input\":\"foo\"}"
    val sourceConfigurationJson = Jsons.jsonNode("{}")
    val sourceUUID = UUID.randomUUID()
    val syncResourceReqs =
      mockk<SyncResourceRequirements> {
        every { configKey } returns resourceRequirementsConfigKey
      }
    val workspaceUUID = UUID.randomUUID()

    val destinationLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val jobRunConfig = mockk<JobRunConfig>()
    val refreshSchemaOutput = mockk<RefreshSchemaActivityOutput>()
    val sourceLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { connectionContext } returns connectionCtx
        every { connectionId } returns connectionUUID
        every { destinationConfiguration } returns destinationConfigurationJson
        every { destinationId } returns destinationUUID
        every { includesFiles } returns shouldIncludeFiles
        every { isReset } returns shouldReset
        every { namespaceDefinition } returns namespaceDefinitionType
        every { networkSecurityTokens } returns securityTokens
        every { prefix } returns prefixString
        every { namespaceFormat } returns namespaceFormatString
        every { omitFileTransferEnvVar } returns shouldOmitFieTransferEnvVar
        every { sourceConfiguration } returns sourceConfigurationJson
        every { sourceId } returns sourceUUID
        every { syncResourceRequirements } returns syncResourceReqs
        every { workspaceId } returns workspaceUUID
      }
    val taskQueue = "test-task-queue"
    val activity =
      GenerateReplicationActivityInputActivityImpl(
        airbyteApiClient = airbyteApiClient,
        featureFlagClient = featureFlagClient,
        replicationFeatureFlags = replicationFeatureFlags,
      )

    val input =
      activity.generate(
        syncInput = syncInput,
        jobRunConfig = jobRunConfig,
        sourceLauncherConfig = sourceLauncherConfig,
        destinationLauncherConfig = destinationLauncherConfig,
        taskQueue = taskQueue,
        refreshSchemaOutput = refreshSchemaOutput,
        signalInput = signalInput,
      )

    assertEquals(connectionCtx, input.connectionContext)
    assertEquals(connectionUUID, input.connectionId)
    assertEquals(destinationConfigurationJson, input.destinationConfiguration)
    assertEquals(destinationDefinitionUUID, input.connectionContext?.destinationDefinitionId)
    assertEquals(destinationUUID, input.destinationId)
    assertEquals(destinationLauncherConfig, input.destinationLauncherConfig)
    assertEquals(destinationSupportRefreshes, input.supportsRefreshes)
    assertEquals(heartbeatMaxSecondsBetweenMessages, input.heartbeatMaxSecondsBetweenMessages)
    assertEquals(jobRunConfig, input.jobRunConfig)
    assertEquals(namespaceDefinitionType, input.namespaceDefinition)
    assertEquals(namespaceFormatString, input.namespaceFormat)
    assertEquals(prefixString, input.prefix)
    assertEquals(refreshSchemaOutput, input.schemaRefreshOutput)
    assertEquals(resolvedFeatureFlags, input.featureFlags)
    assertEquals(securityTokens, input.networkSecurityTokens)
    assertEquals(shouldOmitFieTransferEnvVar, input.omitFileTransferEnvVar)
    assertEquals(shouldIncludeFiles, input.includesFiles)
    assertEquals(shouldReset, input.isReset)
    assertEquals(signalInput, input.signalInput)
    assertEquals(sourceConfigurationJson, input.sourceConfiguration)
    assertEquals(sourceDefinitionUUID, input.connectionContext?.sourceDefinitionId)
    assertEquals(sourceLauncherConfig, input.sourceLauncherConfig)
    assertEquals(sourceUUID, input.sourceId)
    assertEquals(syncResourceReqs, input.syncResourceRequirements)
    assertEquals(taskQueue, input.taskQueue)
    assertEquals(workspaceUUID, input.workspaceId)
  }

  @Test
  @Suppress("UNCHECKED_CAST")
  fun testGeneratingReplicationActivityInputWithSourceDefinitionRetrievalFailure() {
    val destinationDefinitionUUID = UUID.randomUUID()
    val destinationSupportRefreshes = true
    val sourceDefinitionUUID = UUID.randomUUID()
    val heartbeatMaxSecondsBetweenMessages = DEFAULT_MAX_HEARTBEAT_SECONDS_BETWEEN_MESSAGES
    val resolveActorDefinitionVersionResponse =
      mockk<ResolveActorDefinitionVersionResponse> {
        every { supportRefreshes } returns destinationSupportRefreshes
        every { connectorIPCOptions } returns null
      }
    val actorDefinitionVersionApiClient =
      mockk<ActorDefinitionVersionApi> {
        every { resolveActorDefinitionVersionByTag(any()) } returns resolveActorDefinitionVersionResponse
      }
    val sourceDefinitionApiClient =
      mockk<SourceDefinitionApi> {
        every { getSourceDefinition(any()) } throws IOException("Failed to retrieve source definition.")
      }
    val airbyteApiClient =
      mockk<AirbyteApiClient> {
        every { actorDefinitionVersionApi } returns actorDefinitionVersionApiClient
        every { sourceDefinitionApi } returns sourceDefinitionApiClient
      }
    val replicationFeatureFlags = ReplicationFeatureFlags(listOf(DestinationTimeoutEnabled as Flag<Any>))
    val resolvedFeatureFlags = mapOf(DestinationTimeoutEnabled.key to true)
    val featureFlagClient = TestClient(resolvedFeatureFlags)

    val connectionCtx =
      mockk<ConnectionContext> {
        every { sourceDefinitionId } returns sourceDefinitionUUID
        every { destinationDefinitionId } returns destinationDefinitionUUID
      }
    val connectionUUID = UUID.randomUUID()
    val destinationConfigurationJson = Jsons.jsonNode("{}")
    val image = "airbyte/test:1.2"
    val destinationUUID = UUID.randomUUID()
    val namespaceDefinitionType = JobSyncConfig.NamespaceDefinitionType.SOURCE
    val namespaceFormatString = "test_format"
    val prefixString = "test_prefix"
    val resourceRequirementsConfigKey =
      mockk<SyncResourceRequirementsKey> {
        every { subType } returns "test_sub_type"
      }
    val securityTokens = listOf("token1", "token2")
    val shouldOmitFieTransferEnvVar = true
    val shouldIncludeFiles = true
    val shouldReset = true
    val signalInput = "{\"signal-input\":\"foo\"}"
    val sourceConfigurationJson = Jsons.jsonNode("{}")
    val sourceUUID = UUID.randomUUID()
    val syncResourceReqs =
      mockk<SyncResourceRequirements> {
        every { configKey } returns resourceRequirementsConfigKey
      }
    val workspaceUUID = UUID.randomUUID()

    val destinationLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val jobRunConfig = mockk<JobRunConfig>()
    val refreshSchemaOutput = mockk<RefreshSchemaActivityOutput>()
    val sourceLauncherConfig =
      mockk<IntegrationLauncherConfig> {
        every { dockerImage } returns image
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { connectionContext } returns connectionCtx
        every { connectionId } returns connectionUUID
        every { destinationConfiguration } returns destinationConfigurationJson
        every { destinationId } returns destinationUUID
        every { includesFiles } returns shouldIncludeFiles
        every { isReset } returns shouldReset
        every { namespaceDefinition } returns namespaceDefinitionType
        every { networkSecurityTokens } returns securityTokens
        every { prefix } returns prefixString
        every { namespaceFormat } returns namespaceFormatString
        every { omitFileTransferEnvVar } returns shouldOmitFieTransferEnvVar
        every { sourceConfiguration } returns sourceConfigurationJson
        every { sourceId } returns sourceUUID
        every { syncResourceRequirements } returns syncResourceReqs
        every { workspaceId } returns workspaceUUID
      }
    val taskQueue = "test-task-queue"
    val activity =
      GenerateReplicationActivityInputActivityImpl(
        airbyteApiClient = airbyteApiClient,
        featureFlagClient = featureFlagClient,
        replicationFeatureFlags = replicationFeatureFlags,
      )

    val input =
      activity.generate(
        syncInput = syncInput,
        jobRunConfig = jobRunConfig,
        sourceLauncherConfig = sourceLauncherConfig,
        destinationLauncherConfig = destinationLauncherConfig,
        taskQueue = taskQueue,
        refreshSchemaOutput = refreshSchemaOutput,
        signalInput = signalInput,
      )

    assertEquals(connectionCtx, input.connectionContext)
    assertEquals(connectionUUID, input.connectionId)
    assertEquals(destinationConfigurationJson, input.destinationConfiguration)
    assertEquals(destinationDefinitionUUID, input.connectionContext?.destinationDefinitionId)
    assertEquals(destinationUUID, input.destinationId)
    assertEquals(destinationLauncherConfig, input.destinationLauncherConfig)
    assertEquals(destinationSupportRefreshes, input.supportsRefreshes)
    assertEquals(heartbeatMaxSecondsBetweenMessages, input.heartbeatMaxSecondsBetweenMessages)
    assertEquals(jobRunConfig, input.jobRunConfig)
    assertEquals(namespaceDefinitionType, input.namespaceDefinition)
    assertEquals(namespaceFormatString, input.namespaceFormat)
    assertEquals(prefixString, input.prefix)
    assertEquals(refreshSchemaOutput, input.schemaRefreshOutput)
    assertEquals(resolvedFeatureFlags, input.featureFlags)
    assertEquals(securityTokens, input.networkSecurityTokens)
    assertEquals(shouldOmitFieTransferEnvVar, input.omitFileTransferEnvVar)
    assertEquals(shouldIncludeFiles, input.includesFiles)
    assertEquals(shouldReset, input.isReset)
    assertEquals(signalInput, input.signalInput)
    assertEquals(sourceConfigurationJson, input.sourceConfiguration)
    assertEquals(sourceDefinitionUUID, input.connectionContext?.sourceDefinitionId)
    assertEquals(sourceLauncherConfig, input.sourceLauncherConfig)
    assertEquals(sourceUUID, input.sourceId)
    assertEquals(syncResourceReqs, input.syncResourceRequirements)
    assertEquals(taskQueue, input.taskQueue)
    assertEquals(workspaceUUID, input.workspaceId)
  }

  companion object {
    @JvmStatic
    fun signalInputProvider(): List<String?> = listOf("{\"signal-input\":\"foo\"}", null)
  }
}
