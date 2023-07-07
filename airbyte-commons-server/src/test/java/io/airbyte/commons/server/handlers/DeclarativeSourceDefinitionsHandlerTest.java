/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.DeclarativeManifestsReadList;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.DeclarativeSourceManifest;
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody;
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeclarativeSourceDefinitionsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final Long A_VERSION = 32L;
  private static final Long ANOTHER_VERSION = 99L;
  private static final String A_DESCRIPTION = "a description";
  private static final JsonNode A_MANIFEST;
  private static final JsonNode A_SPEC;

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigRepository configRepository;
  private DeclarativeSourceManifestInjector manifestInjector;
  private ConnectorSpecification adaptedConnectorSpecification;
  private ActorDefinitionConfigInjection configInjection;

  private DeclarativeSourceDefinitionsHandler handler;

  @BeforeEach
  void setUp() throws JsonProcessingException {
    configRepository = mock(ConfigRepository.class);
    manifestInjector = mock(DeclarativeSourceManifestInjector.class);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);
    configInjection = mock(ActorDefinitionConfigInjection.class);

    handler = new DeclarativeSourceDefinitionsHandler(configRepository, manifestInjector);
  }

  @Test
  void givenSourceNotAvailableInWorkspaceWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() throws IOException {
    when(configRepository.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID)).thenReturn(false);
    assertThrows(DeclarativeSourceNotFoundException.class, () -> handler.createDeclarativeSourceDefinitionManifest(
        new DeclarativeSourceDefinitionCreateManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID)));
  }

  @Test
  void givenNoDeclarativeManifestForSourceDefinitionIdWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    when(configRepository.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID)).thenReturn(Stream.of());

    assertThrows(SourceIsNotDeclarativeException.class,
        () -> handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
            .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .declarativeManifest(anyDeclarativeManifest().version(A_VERSION))));
  }

  @Test
  void givenVersionAlreadyExistsWhenCreateDeclarativeSourceDefinitionManifestThenThrowException() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    when(configRepository.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
        .thenReturn(Stream.of(new DeclarativeManifest().withVersion(A_VERSION)));

    assertThrows(ValueConflictKnownException.class,
        () -> handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
            .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .declarativeManifest(anyDeclarativeManifest().version(A_VERSION))));
  }

  @Test
  void givenSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();
    when(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC)).thenReturn(adaptedConnectorSpecification);
    when(manifestInjector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)).thenReturn(configInjection);

    handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .setAsActiveManifest(true)
        .declarativeManifest(anyDeclarativeManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(manifestInjector, times(1)).addInjectedDeclarativeManifest(A_SPEC);
    verify(configRepository, times(1)).createDeclarativeManifestAsActiveVersion(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)),
        eq(configInjection),
        eq(adaptedConnectorSpecification));
  }

  @Test
  void givenNotSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();

    handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .setAsActiveManifest(false)
        .declarativeManifest(anyDeclarativeManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).insertDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(configRepository, times(0)).createDeclarativeManifestAsActiveVersion(any(), any(), any());
  }

  @Test
  void whenCreateDeclarativeSourceDefinitionManifestThenManifestDraftDeleted() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();

    handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .workspaceId(A_WORKSPACE_ID)
        .setAsActiveManifest(false)
        .declarativeManifest(anyDeclarativeManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).deleteManifestDraftForActorDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID);
  }

  @Test
  void givenSourceNotAvailableInWorkspaceWhenUpdateDeclarativeManifestVersionThenThrowException() throws IOException {
    when(configRepository.workspaceCanUseCustomDefinition(A_SOURCE_DEFINITION_ID, A_WORKSPACE_ID)).thenReturn(false);
    assertThrows(DeclarativeSourceNotFoundException.class, () -> handler.updateDeclarativeManifestVersion(
        new UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION)));
  }

  @Test
  void givenNoDeclarativeManifestForSourceDefinitionIdWhenUpdateDeclarativeManifestVersionThenThrowException() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    when(configRepository.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID)).thenReturn(Stream.of());

    assertThrows(SourceIsNotDeclarativeException.class, () -> handler.updateDeclarativeManifestVersion(
        new UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION)));
  }

  @Test
  void givenNotFoundWhenUpdateDeclarativeManifestVersionThenThrowException() throws IOException, ConfigNotFoundException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();
    doThrow(ConfigNotFoundException.class).when(configRepository).getDeclarativeManifestByActorDefinitionIdAndVersion(any(), anyLong());

    assertThrows(ConfigNotFoundException.class, () -> handler.updateDeclarativeManifestVersion(
        new UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION)));
  }

  @Test
  void whenUpdateDeclarativeManifestVersionThenSetDeclarativeSourceActiveVersion() throws IOException, ConfigNotFoundException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();
    when(configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(A_SOURCE_DEFINITION_ID, A_VERSION))
        .thenReturn(new DeclarativeManifest()
            .withVersion(A_VERSION)
            .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
            .withManifest(A_MANIFEST)
            .withSpec(A_SPEC));
    when(manifestInjector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)).thenReturn(configInjection);
    when(manifestInjector.createDeclarativeManifestConnectorSpecification(A_SPEC)).thenReturn(adaptedConnectorSpecification);

    handler.updateDeclarativeManifestVersion(
        new UpdateActiveManifestRequestBody().sourceDefinitionId(A_SOURCE_DEFINITION_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION));

    verify(configRepository, times(1)).setDeclarativeSourceActiveVersion(A_SOURCE_DEFINITION_ID, A_VERSION, configInjection,
        adaptedConnectorSpecification);
  }

  @Test
  @DisplayName("listManifestVersions should return a list of all manifest versions with their descriptions and status")
  void testListManifestVersions() throws IOException, ConfigNotFoundException {
    final UUID sourceDefinitionId = UUID.randomUUID();
    givenSourceDefinitionAvailableInWorkspace();

    final DeclarativeManifest manifest1 = new DeclarativeManifest().withVersion(1L).withDescription("first version");
    final DeclarativeManifest manifest2 = new DeclarativeManifest().withVersion(2L).withDescription("second version");
    final DeclarativeManifest manifest3 = new DeclarativeManifest().withVersion(3L).withDescription("third version");

    when(configRepository.getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId)).thenReturn(Stream.of(manifest1, manifest2, manifest3));
    when(configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(sourceDefinitionId)).thenReturn(manifest2);

    final DeclarativeManifestsReadList response =
        handler.listManifestVersions(new ListDeclarativeManifestsRequestBody().sourceDefinitionId(sourceDefinitionId));
    assertEquals(3, response.getManifestVersions().size());

    assertFalse(response.getManifestVersions().get(0).getIsActive());
    assertTrue(response.getManifestVersions().get(1).getIsActive());
    assertFalse(response.getManifestVersions().get(2).getIsActive());

    assertEquals(manifest1.getDescription(), response.getManifestVersions().get(0).getDescription());
    assertEquals(manifest2.getDescription(), response.getManifestVersions().get(1).getDescription());
    assertEquals(manifest3.getDescription(), response.getManifestVersions().get(2).getDescription());

    assertEquals(manifest1.getVersion(), response.getManifestVersions().get(0).getVersion());
    assertEquals(manifest2.getVersion(), response.getManifestVersions().get(1).getVersion());
    assertEquals(manifest3.getVersion(), response.getManifestVersions().get(2).getVersion());
  }

  private void givenSourceDefinitionAvailableInWorkspace() throws IOException {
    when(configRepository.workspaceCanUseCustomDefinition(any(), any())).thenReturn(true);
  }

  private void givenSourceIsDeclarative() throws IOException {
    when(configRepository.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
        .thenReturn(Stream.of(new DeclarativeManifest().withVersion(ANOTHER_VERSION)));
  }

  private static DeclarativeSourceManifest anyDeclarativeManifest() {
    return new DeclarativeSourceManifest().version(A_VERSION);
  }

}
