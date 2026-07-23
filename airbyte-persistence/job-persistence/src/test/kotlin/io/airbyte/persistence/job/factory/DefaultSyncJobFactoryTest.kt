/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.DefaultJobCreator
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class DefaultSyncJobFactoryTest {
  @Test
  fun createSyncJobFromConnectionId() {
    val sourceDefinitionId = UUID.randomUUID()
    val destinationDefinitionId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val operationId = UUID.randomUUID()
    val workspaceWebhookConfigId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val workspaceWebhookName = "test-webhook-name"
    val persistedWebhookConfigs =
      deserialize(
        String.format(
          "{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
          workspaceWebhookConfigId,
          workspaceWebhookName,
        ),
      )
    val sourceConfig = ObjectMapper().readTree("{\"source\": true }")
    val destinationConfig = ObjectMapper().readTree("{\"destination\": true }")
    val configAfterInjection = ObjectMapper().readTree("{\"injected\": true }")
    val jobCreator = mockk<DefaultJobCreator>()
    val workspaceHelper = mockk<WorkspaceHelper>()
    val actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    val sourceService = mockk<SourceService>()
    val destinationService = mockk<DestinationService>()
    val connectionService = mockk<ConnectionService>()
    val operationService = mockk<OperationService>()
    val workspaceService = mockk<WorkspaceService>()

    val jobId = 11L

    val operation = StandardSyncOperation().withOperationId(operationId)
    val operations = listOf(operation)
    val standardSync =
      StandardSync()
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(listOf(operationId))

    val sourceConnection =
      SourceConnection()
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withSourceId(sourceId)
        .withConfiguration(sourceConfig)
    val destinationConnection =
      DestinationConnection()
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDestinationId(destinationId)
        .withConfiguration(destinationConfig)

    val srcDockerRepo = "srcrepo"
    val srcDockerTag = "tag"
    val srcDockerImage = "$srcDockerRepo:$srcDockerTag"
    val srcDockerImageIsDefault = true
    val srcProtocolVersion = Version("0.3.1")

    val dstDockerRepo = "dstrepo"
    val dstDockerTag = "tag"
    val dstDockerImage = "$dstDockerRepo:$dstDockerTag"
    val dstDockerImageIsDefault = true
    val dstProtocolVersion = Version("0.3.2")
    val standardSourceDefinition =
      StandardSourceDefinition().withSourceDefinitionId(sourceDefinitionId)
    val standardDestinationDefinition =
      StandardDestinationDefinition().withDestinationDefinitionId(destinationDefinitionId)

    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(srcDockerRepo)
        .withDockerImageTag(srcDockerTag)
        .withProtocolVersion(srcProtocolVersion.serialize())
    every { actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId, sourceId) } returns sourceVersion
    every { actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId) } returns sourceVersion
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(dstDockerRepo)
        .withDockerImageTag(dstDockerTag)
        .withProtocolVersion(dstProtocolVersion.serialize())
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        workspaceId,
        destinationId,
      )
    } returns destinationVersion
    every { actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId) } returns destinationVersion

    every { workspaceHelper.getWorkspaceForSourceId(sourceId) } returns workspaceId
    every { connectionService.getStandardSync(connectionId) } returns standardSync
    every { sourceService.getSourceConnection(sourceId) } returns sourceConnection
    every { destinationService.getDestinationConnection(destinationId) } returns destinationConnection
    every { operationService.getStandardSyncOperation(operationId) } returns operation
    every {
      jobCreator.createSyncJob(
        any<SourceConnection>(),
        any<DestinationConnection>(),
        standardSync,
        srcDockerImage,
        srcDockerImageIsDefault,
        srcProtocolVersion,
        dstDockerImage,
        dstDockerImageIsDefault,
        dstProtocolVersion,
        operations,
        persistedWebhookConfigs,
        standardSourceDefinition,
        standardDestinationDefinition,
        sourceVersion,
        destinationVersion,
        workspaceId,
        true,
      )
    } returns Optional.of(jobId)
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns standardSourceDefinition

    every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns standardDestinationDefinition

    every { workspaceService.getStandardWorkspaceNoSecrets(any<UUID>(), true) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withWebhookOperationConfigs(persistedWebhookConfigs)

    val configInjector = mockk<ConfigInjector>()
    every { configInjector.injectConfig(sourceConfig, sourceDefinitionId) } returns configAfterInjection
    every { configInjector.injectConfig(destinationConfig, destinationDefinitionId) } returns destinationConfig

    val oAuthConfigSupplier = mockk<OAuthConfigSupplier>()
    every {
      oAuthConfigSupplier.injectSourceOAuthParameters(
        any<UUID>(),
        any<UUID>(),
        any<UUID>(),
        any<JsonNode>(),
      )
    } answers { arg(3) }
    every {
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        any<UUID>(),
        any<UUID>(),
        any<UUID>(),
        any<JsonNode>(),
      )
    } answers { arg(3) }

    val factory: SyncJobFactory =
      DefaultSyncJobFactory(
        true,
        jobCreator,
        oAuthConfigSupplier,
        configInjector,
        workspaceHelper,
        actorDefinitionVersionHelper,
        sourceService,
        destinationService,
        connectionService,
        operationService,
        workspaceService,
      )
    val actualJobId = factory.createSync(connectionId, true)
    Assertions.assertEquals(jobId, actualJobId)

    val sourceConnectionCaptor = slot<SourceConnection>()
    val destinationConnectionCaptor = slot<DestinationConnection>()
    verify {
      jobCreator.createSyncJob(
        capture(sourceConnectionCaptor),
        capture(destinationConnectionCaptor),
        standardSync,
        srcDockerImage,
        srcDockerImageIsDefault,
        srcProtocolVersion,
        dstDockerImage,
        dstDockerImageIsDefault,
        dstProtocolVersion,
        operations,
        persistedWebhookConfigs,
        standardSourceDefinition,
        standardDestinationDefinition,
        sourceVersion,
        destinationVersion,
        workspaceId,
        true,
      )
    }

    Assertions.assertEquals(configAfterInjection, sourceConnectionCaptor.captured.configuration)
    Assertions.assertEquals(destinationConfig, destinationConnectionCaptor.captured.configuration)
    verify { configInjector.injectConfig(sourceConfig, sourceDefinitionId) }
    verify { configInjector.injectConfig(destinationConfig, destinationDefinitionId) }
    verify { actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId, sourceId) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId) }
  }

  @Test
  fun testImageIsDefault() {
    val jobCreator = mockk<DefaultJobCreator>()
    val oAuthConfigSupplier = mockk<OAuthConfigSupplier>()
    val configInjector = mockk<ConfigInjector>()
    val workspaceHelper = mockk<WorkspaceHelper>()
    val actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    val sourceService = mockk<SourceService>()
    val destinationService = mockk<DestinationService>()
    val connectionService = mockk<ConnectionService>()
    val operationService = mockk<OperationService>()
    val workspaceService = mockk<WorkspaceService>()
    val jobFactory =
      DefaultSyncJobFactory(
        true,
        jobCreator,
        oAuthConfigSupplier,
        configInjector,
        workspaceHelper,
        actorDefinitionVersionHelper,
        sourceService,
        destinationService,
        connectionService,
        operationService,
        workspaceService,
      )
    var version = ActorDefinitionVersion()
    version.dockerRepository = "repo"
    version.dockerImageTag = "tag"
    // null input image
    Assertions.assertTrue(jobFactory.imageIsDefault(null, version))
    // Same versions
    Assertions.assertTrue(jobFactory.imageIsDefault("repo:tag", version))
    // Different versions
    Assertions.assertFalse(jobFactory.imageIsDefault("repo:latest", version))
    // ActorDefinitionVersion is null
    Assertions.assertTrue(jobFactory.imageIsDefault("repo:tag", null))
    // null repo & tag
    version = ActorDefinitionVersion()
    Assertions.assertTrue(jobFactory.imageIsDefault("repo:tag", version))
  }
}
