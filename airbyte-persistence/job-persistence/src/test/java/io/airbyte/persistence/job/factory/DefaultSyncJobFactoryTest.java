/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.airbyte.config.persistence.ConfigRepository;
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
  void createSyncJobFromConnectionId() throws JsonValidationException, ConfigNotFoundException, IOException {
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
    final ConfigRepository configRepository = mock(ConfigRepository.class);
    final WorkspaceHelper workspaceHelper = mock(WorkspaceHelper.class);
    final ActorDefinitionVersionHelper actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
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
    final Version srcProtocolVersion = new Version("0.3.1");

    final String dstDockerRepo = "dstrepo";
    final String dstDockerTag = "tag";
    final String dstDockerImage = dstDockerRepo + ":" + dstDockerTag;
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
    final ActorDefinitionVersion destinationVersion = new ActorDefinitionVersion()
        .withDockerRepository(dstDockerRepo)
        .withDockerImageTag(dstDockerTag)
        .withProtocolVersion(dstProtocolVersion.serialize());
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId))
        .thenReturn(destinationVersion);

    when(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId);
    when(configRepository.getStandardSync(connectionId)).thenReturn(standardSync);
    when(configRepository.getSourceConnection(sourceId)).thenReturn(sourceConnection);
    when(configRepository.getDestinationConnection(destinationId)).thenReturn(destinationConnection);
    when(configRepository.getStandardSyncOperation(operationId)).thenReturn(operation);
    when(
        jobCreator.createSyncJob(sourceConnection, destinationConnection, standardSync, srcDockerImage, srcProtocolVersion, dstDockerImage,
            dstProtocolVersion, operations,
            persistedWebhookConfigs, standardSourceDefinition, standardDestinationDefinition, sourceVersion, destinationVersion, workspaceId))
                .thenReturn(Optional.of(jobId));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId))
        .thenReturn(standardSourceDefinition);

    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId))
        .thenReturn(standardDestinationDefinition);

    when(configRepository.getStandardWorkspaceNoSecrets(any(), eq(true))).thenReturn(
        new StandardWorkspace().withWorkspaceId(workspaceId).withWebhookOperationConfigs(persistedWebhookConfigs));

    final ConfigInjector configInjector = mock(ConfigInjector.class);
    when(configInjector.injectConfig(any(), any())).thenAnswer(i -> i.getArguments()[0]);
    when(configInjector.injectConfig(any(), eq(sourceDefinitionId))).thenReturn(configAfterInjection);

    final OAuthConfigSupplier oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    when(oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), any())).thenAnswer(i -> i.getArguments()[3]);
    when(oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), any())).thenAnswer(i -> i.getArguments()[3]);

    final SyncJobFactory factory =
        new DefaultSyncJobFactory(true, jobCreator, configRepository, oAuthConfigSupplier, configInjector, workspaceHelper,
            actorDefinitionVersionHelper);
    final long actualJobId = factory.create(connectionId);
    assertEquals(jobId, actualJobId);

    verify(jobCreator)
        .createSyncJob(sourceConnection, destinationConnection, standardSync, srcDockerImage, srcProtocolVersion, dstDockerImage, dstProtocolVersion,
            operations, persistedWebhookConfigs,
            standardSourceDefinition, standardDestinationDefinition, sourceVersion, destinationVersion, workspaceId);

    assertEquals(configAfterInjection, sourceConnection.getConfiguration());
    verify(configInjector).injectConfig(sourceConfig, sourceDefinitionId);
    verify(configInjector).injectConfig(destinationConfig, destinationDefinitionId);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, workspaceId, sourceId);
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, workspaceId, destinationId);
  }

}
