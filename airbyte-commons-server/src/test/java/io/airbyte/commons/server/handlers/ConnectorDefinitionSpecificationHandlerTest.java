/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.SourceService;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.AdvancedAuth.AuthFlowType;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.CollectionAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link ConnectorDefinitionSpecificationHandler}.
 */
class ConnectorDefinitionSpecificationHandlerTest {

  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private JobConverter jobConverter;
  private ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;

  private static final String CONNECTOR_URL = "https://google.com";
  private static final String DESTINATION_DOCKER_TAG = "tag";
  private static final String NAME = "name";
  private static final String SOURCE_DOCKER_REPO = "srcimage";
  private static final String SOURCE_DOCKER_TAG = "tag";

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION = new ConnectorSpecification()
      .withDocumentationUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  private static final ConnectorSpecification CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL = new ConnectorSpecification()
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  private SourceService sourceService;
  private DestinationService destinationService;
  private OAuthService oAuthService;

  @BeforeEach
  void setup() {
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    jobConverter = mock(JobConverter.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);
    oAuthService = mock(OAuthService.class);

    connectorDefinitionSpecificationHandler =
        new ConnectorDefinitionSpecificationHandler(actorDefinitionVersionHelper, jobConverter, sourceService, destinationService, oAuthService);
  }

  @Test
  void testGetDestinationSpecForDestinationId()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    final DestinationIdRequestBody destinationIdRequestBody =
        new DestinationIdRequestBody()
            .destinationId(destinationId);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionId);
    when(destinationService.getDestinationConnection(destinationId)).thenReturn(
        new DestinationConnection()
            .withDestinationId(destinationId)
            .withWorkspaceId(workspaceId)
            .withDestinationDefinitionId(destinationDefinitionId));
    when(destinationService.getStandardDestinationDefinition(destinationDefinitionId))
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
  void testGetSourceSpecWithoutDocs()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPO)
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL));

    final SourceDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(sourceService).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpecForSourceId()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();

    final SourceIdRequestBody sourceIdRequestBody =
        new SourceIdRequestBody()
            .sourceId(sourceId);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionId);
    when(sourceService.getSourceConnection(sourceId)).thenReturn(
        new SourceConnection()
            .withSourceId(sourceId)
            .withWorkspaceId(workspaceId)
            .withSourceDefinitionId(sourceDefinitionId));
    when(sourceService.getStandardSourceDefinition(sourceDefinitionId))
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
  void testGetDestinationSpec()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    when(destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId()))
            .thenReturn(new ActorDefinitionVersion()
                .withDockerImageTag(DESTINATION_DOCKER_TAG)
                .withSpec(CONNECTOR_SPECIFICATION));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId);

    verify(destinationService).getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @Test
  void testGetSourceSpec()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    when(sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerImageTag(SOURCE_DOCKER_TAG)
            .withSpec(CONNECTOR_SPECIFICATION));

    final SourceDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId);

    verify(sourceService).getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());
    verify(actorDefinitionVersionHelper).getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification());
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  void testDestinationSyncModeEnrichment(final boolean supportsRefreshes)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    when(destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId()))
            .thenReturn(new ActorDefinitionVersion()
                .withDockerImageTag(DESTINATION_DOCKER_TAG)
                .withSpec(new ConnectorSpecification()
                    .withDocumentationUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
                    .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
                    .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()))
                    .withSupportedDestinationSyncModes(List.of(io.airbyte.protocol.models.DestinationSyncMode.APPEND,
                        io.airbyte.protocol.models.DestinationSyncMode.APPEND_DEDUP, io.airbyte.protocol.models.DestinationSyncMode.OVERWRITE)))
                .withSupportsRefreshes(supportsRefreshes));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId);

    verify(destinationService).getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    if (supportsRefreshes) {
      CollectionAssert.assertThatCollection(response.getSupportedDestinationSyncModes()).containsExactlyInAnyOrderElementsOf(List.of(
          DestinationSyncMode.APPEND, DestinationSyncMode.APPEND_DEDUP, DestinationSyncMode.OVERWRITE, DestinationSyncMode.OVERWRITE_DEDUP));
    } else {
      CollectionAssert.assertThatCollection(response.getSupportedDestinationSyncModes()).containsExactlyInAnyOrderElementsOf(List.of(
          DestinationSyncMode.APPEND, DestinationSyncMode.APPEND_DEDUP, DestinationSyncMode.OVERWRITE));
    }
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  void testDestinationSyncModeEnrichmentWithoutOverwrite(final boolean supportsRefreshes)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID());

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    when(destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId()))
            .thenReturn(new ActorDefinitionVersion()
                .withDockerImageTag(DESTINATION_DOCKER_TAG)
                .withSpec(new ConnectorSpecification()
                    .withDocumentationUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
                    .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
                    .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()))
                    .withSupportedDestinationSyncModes(
                        List.of(io.airbyte.protocol.models.DestinationSyncMode.APPEND, io.airbyte.protocol.models.DestinationSyncMode.APPEND_DEDUP)))
                .withSupportsRefreshes(supportsRefreshes));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId);

    verify(destinationService).getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());
    verify(actorDefinitionVersionHelper).getDestinationVersion(destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    CollectionAssert.assertThatCollection(response.getSupportedDestinationSyncModes()).containsExactlyInAnyOrderElementsOf(List.of(
        DestinationSyncMode.APPEND, DestinationSyncMode.APPEND_DEDUP));
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  void getDestinationSpecificationReadAdvancedAuth(final boolean advancedAuthGlobalCredentialsAvailable) throws IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    when(oAuthService.getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId))
        .thenReturn(advancedAuthGlobalCredentialsAvailable ? Optional.of(new DestinationOAuthParameter()) : Optional.empty());

    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId =
        new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId).workspaceId(workspaceId);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId());

    final ConnectorSpecification connectorSpecification = new ConnectorSpecification()
        .withDocumentationUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
        .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
        .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()))
        .withAdvancedAuth(new AdvancedAuth().withAuthFlowType(AuthFlowType.OAUTH_2_0).withOauthConfigSpecification(new OAuthConfigSpecification()));

    final DestinationDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getDestinationSpecificationRead(destinationDefinition, connectorSpecification, true, workspaceId);

    verify(oAuthService).getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId);
    assertEquals(advancedAuthGlobalCredentialsAvailable, response.getAdvancedAuthGlobalCredentialsAvailable());
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  void getSourceSpecificationReadAdvancedAuth(final boolean advancedAuthGlobalCredentialsAvailable) throws IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    when(oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId))
        .thenReturn(advancedAuthGlobalCredentialsAvailable ? Optional.of(new SourceOAuthParameter()) : Optional.empty());

    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId =
        new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(workspaceId);
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId());

    final ConnectorSpecification connectorSpecification = new ConnectorSpecification()
        .withDocumentationUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
        .withChangelogUrl(Exceptions.toRuntime(() -> new URI(CONNECTOR_URL)))
        .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()))
        .withAdvancedAuth(new AdvancedAuth().withAuthFlowType(AuthFlowType.OAUTH_2_0).withOauthConfigSpecification(new OAuthConfigSpecification()));

    final SourceDefinitionSpecificationRead response =
        connectorDefinitionSpecificationHandler.getSourceSpecificationRead(sourceDefinition, connectorSpecification, workspaceId);

    verify(oAuthService).getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId);
    assertEquals(advancedAuthGlobalCredentialsAvailable, response.getAdvancedAuthGlobalCredentialsAvailable());
  }

}
