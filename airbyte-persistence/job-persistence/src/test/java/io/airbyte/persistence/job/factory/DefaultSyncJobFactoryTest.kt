/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.DefaultJobCreator
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.stubbing.Answer
import java.io.IOException
import java.util.Optional
import java.util.UUID

internal class DefaultSyncJobFactoryTest {
  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
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
    val jobCreator = mock<DefaultJobCreator>()
    val workspaceHelper = mock<WorkspaceHelper>()
    val actorDefinitionVersionHelper = mock<ActorDefinitionVersionHelper>()
    val sourceService = mock<SourceService>()
    val destinationService = mock<DestinationService>()
    val connectionService = mock<ConnectionService>()
    val operationService = mock<OperationService>()
    val workspaceService = mock<WorkspaceService>()

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
    val srcDockerImage = srcDockerRepo + ":" + srcDockerTag
    val srcDockerImageIsDefault = true
    val srcProtocolVersion = Version("0.3.1")

    val dstDockerRepo = "dstrepo"
    val dstDockerTag = "tag"
    val dstDockerImage = dstDockerRepo + ":" + dstDockerTag
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
    whenever(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId, sourceId))
      .thenReturn(sourceVersion)
    whenever(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId))
      .thenReturn(sourceVersion)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(dstDockerRepo)
        .withDockerImageTag(dstDockerTag)
        .withProtocolVersion(dstProtocolVersion.serialize())
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        standardDestinationDefinition,
        workspaceId,
        destinationId,
      ),
    ).thenReturn(destinationVersion)
    whenever(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId))
      .thenReturn(destinationVersion)

    whenever(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId)
    whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)
    whenever(sourceService.getSourceConnection(sourceId)).thenReturn(sourceConnection)
    whenever(destinationService.getDestinationConnection(destinationId)).thenReturn(destinationConnection)
    whenever(operationService.getStandardSyncOperation(operationId)).thenReturn(operation)
    whenever(
      jobCreator.createSyncJob(
        any<SourceConnection>(),
        any<DestinationConnection>(),
        eq(standardSync),
        eq(srcDockerImage),
        eq(srcDockerImageIsDefault),
        eq(srcProtocolVersion),
        eq(dstDockerImage),
        eq(dstDockerImageIsDefault),
        eq(dstProtocolVersion),
        eq(operations),
        eq(persistedWebhookConfigs),
        eq(standardSourceDefinition),
        eq(standardDestinationDefinition),
        eq(sourceVersion),
        eq(destinationVersion),
        eq(workspaceId),
        eq(true),
      ),
    ).thenReturn(Optional.of(jobId))
    whenever(sourceService.getStandardSourceDefinition(sourceDefinitionId))
      .thenReturn(standardSourceDefinition)

    whenever(destinationService.getStandardDestinationDefinition(destinationDefinitionId))
      .thenReturn(standardDestinationDefinition)

    whenever(workspaceService.getStandardWorkspaceNoSecrets(any<UUID>(), eq(true)))
      .thenReturn(
        StandardWorkspace().withWorkspaceId(workspaceId).withWebhookOperationConfigs(persistedWebhookConfigs),
      )

    val configInjector = mock<ConfigInjector>()
    whenever(configInjector.injectConfig(eq(sourceConfig), eq(sourceDefinitionId)))
      .thenReturn(configAfterInjection)
    whenever(configInjector.injectConfig(eq(destinationConfig), eq(destinationDefinitionId)))
      .thenReturn(destinationConfig)

    val oAuthConfigSupplier = mock<OAuthConfigSupplier>()
    whenever(
      oAuthConfigSupplier.injectSourceOAuthParameters(
        any<UUID>(),
        any<UUID>(),
        any<UUID>(),
        any<JsonNode>(),
      ),
    ).thenAnswer(Answer { i: InvocationOnMock? -> i!!.getArguments()[3] })
    whenever(
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        any<UUID>(),
        any<UUID>(),
        any<UUID>(),
        any<JsonNode>(),
      ),
    ).thenAnswer(Answer { i: InvocationOnMock? -> i!!.getArguments()[3] })

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

    val sourceConnectionCaptor = argumentCaptor<SourceConnection>()
    val destinationConnectionCaptor = argumentCaptor<DestinationConnection>()
    verify(jobCreator)
      .createSyncJob(
        sourceConnectionCaptor.capture(),
        destinationConnectionCaptor.capture(),
        eq(standardSync),
        eq(srcDockerImage),
        eq(srcDockerImageIsDefault),
        eq(srcProtocolVersion),
        eq(dstDockerImage),
        eq(dstDockerImageIsDefault),
        eq(dstProtocolVersion),
        eq(operations),
        eq(persistedWebhookConfigs),
        eq(standardSourceDefinition),
        eq(standardDestinationDefinition),
        eq(sourceVersion),
        eq(destinationVersion),
        eq(workspaceId),
        eq(true),
      )

    Assertions.assertEquals(configAfterInjection, sourceConnectionCaptor.firstValue.configuration)
    Assertions.assertEquals(destinationConfig, destinationConnectionCaptor.firstValue.configuration)
    verify(configInjector).injectConfig(sourceConfig, sourceDefinitionId)
    verify(configInjector).injectConfig(destinationConfig, destinationDefinitionId)
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, workspaceId, sourceId)
    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId)
  }

  @Test
  fun testImageIsDefault() {
    val jobCreator = mock<DefaultJobCreator>()
    val oAuthConfigSupplier = mock<OAuthConfigSupplier>()
    val configInjector = mock<ConfigInjector>()
    val workspaceHelper = mock<WorkspaceHelper>()
    val actorDefinitionVersionHelper = mock<ActorDefinitionVersionHelper>()
    val sourceService = mock<SourceService>()
    val destinationService = mock<DestinationService>()
    val connectionService = mock<ConnectionService>()
    val operationService = mock<OperationService>()
    val workspaceService = mock<WorkspaceService>()
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
    version.setDockerRepository("repo")
    version.setDockerImageTag("tag")
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
