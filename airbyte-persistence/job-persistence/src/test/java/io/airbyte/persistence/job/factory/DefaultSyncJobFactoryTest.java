/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.persistence.job.DefaultJobCreator;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultSyncJobFactoryTest {

  @Test
  void createSyncJobFromConnectionId()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID operationId = UUID.randomUUID();
    final UUID workspaceWebhookConfigId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final String workspaceWebhookName = "test-webhook-name";
    final JsonNode persistedWebhookConfigs = Jsons.deserialize(
        String.format("{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
            workspaceWebhookConfigId, workspaceWebhookName));
    final JsonNode sourceConfig = new ObjectMapper().readTree("{\"source\": true }");
    final JsonNode destinationConfig = new ObjectMapper().readTree("{\"destination\": true }");
    final JsonNode configAfterInjection = new ObjectMapper().readTree("{\"injected\": true }");
    final DefaultJobCreator jobCreator = mock(DefaultJobCreator.class);
    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final ActorDefinitionVersionHelper actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    final SourceService sourceService = mock(SourceService.class);
    final DestinationService destinationService = mock(DestinationService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final OperationService operationService = mock(OperationService.class);
    final WorkspaceService workspaceService = mock(WorkspaceService.class);

    final long jobId = 11L;

    final StandardSyncOperation operation = new StandardSyncOperation().withOperationId(operationId);
    final List<StandardSyncOperation> operations = List.of(operation);
    final StandardSync standardSync = new StandardSync()
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(operationId));

    final SourceConnection sourceConnection = new SourceConnection()
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withConfiguration(sourceConfig);
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withConfiguration(destinationConfig);

    final String srcDockerRepo = "srcrepo";
    final String srcDockerTag = "tag";
    final String srcDockerImage = srcDockerRepo + ":" + srcDockerTag;
    final Boolean srcDockerImageIsDefault = true;
    final Version srcProtocolVersion = new Version("0.3.1");

    final String dstDockerRepo = "dstrepo";
    final String dstDockerTag = "tag";
    final String dstDockerImage = dstDockerRepo + ":" + dstDockerTag;
    final Boolean dstDockerImageIsDefault = true;
    final Version dstProtocolVersion = new Version("0.3.2");
    final StandardSourceDefinition standardSourceDefinition =
        new StandardSourceDefinition().withSourceDefinitionId(sourceDefinitionId);
    final StandardDestinationDefinition standardDestinationDefinition =
        new StandardDestinationDefinition().withDestinationDefinitionId(destinationDefinitionId);

    final ActorDefinitionVersion sourceVersion = new ActorDefinitionVersion()
        .withDockerRepository(srcDockerRepo)
        .withDockerImageTag(srcDockerTag)
        .withProtocolVersion(srcProtocolVersion.serialize());
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId, sourceId))
        .thenReturn(sourceVersion);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId))
        .thenReturn(sourceVersion);
    final ActorDefinitionVersion destinationVersion = new ActorDefinitionVersion()
        .withDockerRepository(dstDockerRepo)
        .withDockerImageTag(dstDockerTag)
        .withProtocolVersion(dstProtocolVersion.serialize());
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId))
        .thenReturn(destinationVersion);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId))
        .thenReturn(destinationVersion);

    when(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId);
    when(connectionService.getStandardSync(connectionId)).thenReturn(standardSync);
    when(sourceService.getSourceConnection(sourceId)).thenReturn(sourceConnection);
    when(destinationService.getDestinationConnection(destinationId)).thenReturn(destinationConnection);
    when(operationService.getStandardSyncOperation(operationId)).thenReturn(operation);
    when(
        jobCreator.createSyncJob(sourceConnection, destinationConnection, standardSync, srcDockerImage, srcDockerImageIsDefault, srcProtocolVersion,
            dstDockerImage,
            dstDockerImageIsDefault, dstProtocolVersion, operations,
            persistedWebhookConfigs, standardSourceDefinition, standardDestinationDefinition, sourceVersion, destinationVersion, workspaceId))
                .thenReturn(Optional.of(jobId));
    when(sourceService.getStandardSourceDefinition(sourceDefinitionId))
        .thenReturn(standardSourceDefinition);

    when(destinationService.getStandardDestinationDefinition(destinationDefinitionId))
        .thenReturn(standardDestinationDefinition);

    when(workspaceService.getStandardWorkspaceNoSecrets(any(), eq(true))).thenReturn(
        new StandardWorkspace().withWorkspaceId(workspaceId).withWebhookOperationConfigs(persistedWebhookConfigs));

    final ConfigInjector configInjector = mock(ConfigInjector.class);
    when(configInjector.injectConfig(any(), any())).thenAnswer(i -> i.getArguments()[0]);
    when(configInjector.injectConfig(any(), eq(sourceDefinitionId))).thenReturn(configAfterInjection);

    final OAuthConfigSupplier oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    when(oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), any())).thenAnswer(i -> i.getArguments()[3]);
    when(oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), any())).thenAnswer(i -> i.getArguments()[3]);

    final SyncJobFactory factory =
        new DefaultSyncJobFactory(
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
            workspaceService);
    final long actualJobId = factory.createSync(connectionId);
    assertEquals(jobId, actualJobId);

    verify(jobCreator)
        .createSyncJob(sourceConnection, destinationConnection, standardSync, srcDockerImage, srcDockerImageIsDefault, srcProtocolVersion,
            dstDockerImage, dstDockerImageIsDefault, dstProtocolVersion,
            operations, persistedWebhookConfigs,
            standardSourceDefinition, standardDestinationDefinition, sourceVersion, destinationVersion, workspaceId);

    assertEquals(configAfterInjection, sourceConnection.getConfiguration());
    verify(configInjector).injectConfig(sourceConfig, sourceDefinitionId);
    verify(configInjector).injectConfig(destinationConfig, destinationDefinitionId);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, workspaceId, sourceId);
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId);
  }

  @Test
  void testImageIsDefault() {
    final DefaultJobCreator jobCreator = mock(DefaultJobCreator.class);
    final OAuthConfigSupplier oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    final ConfigInjector configInjector = mock(ConfigInjector.class);
    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final ActorDefinitionVersionHelper actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    final SourceService sourceService = mock(SourceService.class);
    final DestinationService destinationService = mock(DestinationService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final OperationService operationService = mock(OperationService.class);
    final WorkspaceService workspaceService = mock(WorkspaceService.class);
    final DefaultSyncJobFactory jobFactory = new DefaultSyncJobFactory(
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
        workspaceService);
    ActorDefinitionVersion version = new ActorDefinitionVersion();
    version.setDockerRepository("repo");
    version.setDockerImageTag("tag");
    // null input image
    assertTrue(jobFactory.imageIsDefault(null, version));
    // Same versions
    assertTrue(jobFactory.imageIsDefault("repo:tag", version));
    // Different versions
    assertFalse(jobFactory.imageIsDefault("repo:latest", version));
    // ActorDefinitionVersion is null
    assertTrue(jobFactory.imageIsDefault("repo:tag", null));
    // null repo & tag
    version = new ActorDefinitionVersion();
    assertTrue(jobFactory.imageIsDefault("repo:tag", version));
  }

}
