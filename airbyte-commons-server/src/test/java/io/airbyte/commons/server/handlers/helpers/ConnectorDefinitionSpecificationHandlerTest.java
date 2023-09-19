/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectorDefinitionSpecificationHandler}.
 */
class ConnectorDefinitionSpecificationHandlerTest {

  private ConfigRepository configRepository;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private JobConverter jobConverter;
  private ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;

  private static final String DESTINATION_DOCKER_TAG = "tag";
  private static final String NAME = "name";
  private static final String SOURCE_DOCKER_REPO = "srcimage";
  private static final String SOURCE_DOCKER_TAG = "tag";

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION = new ConnectorSpecification()
      .withDocumentationUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL = new ConnectorSpecification()
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    jobConverter = mock(JobConverter.class);

    connectorDefinitionSpecificationHandler =
        new ConnectorDefinitionSpecificationHandler(configRepository, actorDefinitionVersionHelper, jobConverter);
  }

  @Test
  void testGetDestinationSpecForDestinationId() throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    final DestinationIdRequestBody destinationIdRequestBody =
        new DestinationIdRequestBody()
            .destinationId(destinationId);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionId);
    when(configRepository.getDestinationConnection(destinationId)).thenReturn(
        new DestinationConnection()
            .withDestinationId(destinationId)
            .withWorkspaceId(workspaceId)
            .withDestinationDefinitionId(destinationDefinitionId));
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(DESTINATION_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSpecificationForDestinationId(destinationIdRequestBody);

    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition, workspaceId, destinationId);
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpecWithoutDocs() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL));

    final SourceDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpecForSourceId() throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();

    final SourceIdRequestBody sourceIdRequestBody =
        new SourceIdRequestBody()
            .sourceId(sourceId);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionId);
    when(configRepository.getSourceConnection(sourceId)).thenReturn(
        new SourceConnection()
            .withSourceId(sourceId)
            .withWorkspaceId(workspaceId)
            .withSourceDefinitionId(sourceDefinitionId));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final SourceDefinitionSpecificationRead response = connectorDefinitionSpecificationHandler.getSpecificationForSourceId(sourceIdRequestBody);

    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, workspaceId, sourceId);
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetDestinationSpec() throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId()))
            .thenReturn(new ActorDefinitionVersion()
                .withDockerImageTag(DESTINATION_DOCKER_TAG)
                .withSpec(CONNECTOR_SPECIFICATION));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpec() throws JsonValidationException, IOException, ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(configRepository.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final SourceDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(configRepository).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

}
