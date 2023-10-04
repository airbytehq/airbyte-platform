/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.ConnectorDocumentationHandler.LATEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ActorType;
import io.airbyte.api.model.generated.ConnectorDocumentationRead;
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorDocumentationHandlerTest {

  private ConfigRepository configRepository;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private ConnectorDocumentationHandler connectorDocumentationHandler;

  private static final String SOURCE_DOCKER_REPO = "airbyte/source-test";
  private static final String SOURCE_VERSION_OLD = "0.0.1";
  private static final String SOURCE_VERSION_LATEST = "0.0.9";
  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition();
  private static final ActorDefinitionVersion SOURCE_DEFINITION_VERSION_OLD =
      new ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(
          SOURCE_VERSION_OLD);
  private static final ActorDefinitionVersion SOURCE_DEFINITION_VERSION_LATEST =
      new ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(
          SOURCE_VERSION_LATEST);

  private static final String DESTINATION_DOCKER_REPO = "airbyte/destination-test";
  private static final String DESTINATION_VERSION_OLD = "0.0.1";
  private static final String DESTINATION_VERSION_LATEST = "0.0.9";
  private static final StandardDestinationDefinition DESTINATION_DEFINITION = new StandardDestinationDefinition();
  private static final ActorDefinitionVersion DESTINATION_DEFINITION_VERSION_OLD =
      new ActorDefinitionVersion().withDockerRepository(DESTINATION_DOCKER_REPO).withDockerImageTag(
          DESTINATION_VERSION_OLD);
  private static final ActorDefinitionVersion DESTINATION_DEFINITION_VERSION_LATEST =
      new ActorDefinitionVersion().withDockerRepository(DESTINATION_DOCKER_REPO).withDockerImageTag(
          DESTINATION_VERSION_LATEST);

  private static final String FULL_DOC_CONTENTS_OLD = "The full doc contents for the old version";
  private static final String INAPP_DOC_CONTENTS_OLD = "The inapp doc contents for the old version";
  private static final String FULL_DOC_CONTENTS_LATEST = "The full doc contents for the latest version";
  private static final String INAPP_DOC_CONTENTS_LATEST = "The inapp doc contents for the latest version";

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);

    connectorDocumentationHandler =
        new ConnectorDocumentationHandler(configRepository, actorDefinitionVersionHelper, remoteDefinitionsProvider);
  }

  // SOURCE

  @Test
  void testNoSourceDocumentationFound() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(any(), any(), any())).thenReturn(Optional.empty());

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId).actorId(sourceId);

    assertThrows(NotFoundException.class, () -> connectorDocumentationHandler.getConnectorDocumentation(request));
  }

  @Test
  void testGetVersionedInappExistingSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_OLD));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId).actorId(sourceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_OLD);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedFullExistingSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_OLD));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId).actorId(sourceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_OLD);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestInappExistingSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, true)).thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId).actorId(sourceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestFullExistingSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, sourceId)).thenReturn(SOURCE_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_OLD, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, false)).thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE)
        .actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId).actorId(sourceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedInappNewSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(SOURCE_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedFullNewSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(SOURCE_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestInappNewSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(SOURCE_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, true)).thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestFullNewSourceDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, workspaceId, null)).thenReturn(SOURCE_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, SOURCE_VERSION_LATEST, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(SOURCE_DOCKER_REPO, LATEST, false)).thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.SOURCE).actorDefinitionId(sourceDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  // DESTINATION
  @Test
  void testNoDestinationDocumentationFound() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId))
        .thenReturn(DESTINATION_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(any(), any(), any())).thenReturn(Optional.empty());

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId).actorId(destinationId);

    assertThrows(NotFoundException.class, () -> connectorDocumentationHandler.getConnectorDocumentation(request));
  }

  @Test
  void testGetVersionedInappExistingDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId))
        .thenReturn(DESTINATION_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_OLD));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId).actorId(destinationId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_OLD);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedFullExistingDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId))
        .thenReturn(DESTINATION_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_OLD));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId).actorId(destinationId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_OLD);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestInappExistingDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId))
        .thenReturn(DESTINATION_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId).actorId(destinationId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestFullExistingDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, destinationId))
        .thenReturn(DESTINATION_DEFINITION_VERSION_OLD);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_OLD, false)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request = new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION)
        .actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId).actorId(destinationId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedInappNewDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
        .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetVersionedFullNewDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
        .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestInappNewDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
        .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, false))
        .thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, true))
        .thenReturn(Optional.of(INAPP_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(INAPP_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testGetLatestFullNewDestinationDocumentation() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(configRepository.getStandardDestinationDefinition(destinationDefinitionId)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, workspaceId, null))
        .thenReturn(DESTINATION_DEFINITION_VERSION_LATEST);

    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, DESTINATION_VERSION_LATEST, false))
        .thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, true)).thenReturn(Optional.empty());
    when(remoteDefinitionsProvider.getConnectorDocumentation(DESTINATION_DOCKER_REPO, LATEST, false))
        .thenReturn(Optional.of(FULL_DOC_CONTENTS_LATEST));

    final ConnectorDocumentationRequestBody request =
        new ConnectorDocumentationRequestBody().actorType(ActorType.DESTINATION).actorDefinitionId(destinationDefinitionId).workspaceId(workspaceId);

    final ConnectorDocumentationRead expectedResult =
        new ConnectorDocumentationRead().doc(FULL_DOC_CONTENTS_LATEST);
    final ConnectorDocumentationRead actualResult = connectorDocumentationHandler.getConnectorDocumentation(request);

    assertEquals(expectedResult, actualResult);
  }

}
