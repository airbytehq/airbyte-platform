/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.DeclarativeSourceManifest;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeclarativeSourceDefinitionsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final Long A_VERSION = 32L;
  private static final Long ANOTHER_VERSION = 99L;
  private static final String A_DESCRIPTION = "a description";
  private static final String A_DOCUMENTATION_URL = "http://documentation.url";
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
  private ConnectorBuilderSpecAdapter specAdapter;
  private ConnectorSpecification adaptedConnectorSpecification;

  private DeclarativeSourceDefinitionsHandler handler;

  @BeforeEach
  void setUp() throws JsonProcessingException {
    configRepository = mock(ConfigRepository.class);
    specAdapter = mock(ConnectorBuilderSpecAdapter.class);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);

    handler = new DeclarativeSourceDefinitionsHandler(configRepository, specAdapter);
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
        .thenReturn(Stream.of(new DeclarativeManifest().withVersion(A_VERSION.longValue())));

    assertThrows(ValueConflictKnownException.class,
        () -> handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
            .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .declarativeManifest(anyDeclarativeManifest().version(A_VERSION))));
  }

  @Test
  void givenSetAsActiveWhenCreateDeclarativeSourceDefinitionManifestThenCreateDeclarativeManifest() throws IOException {
    givenSourceDefinitionAvailableInWorkspace();
    givenSourceIsDeclarative();
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL);

    handler.createDeclarativeSourceDefinitionManifest(new DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .setAsActiveManifest(true)
        .declarativeManifest(anyDeclarativeManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1))
        .updateDeclarativeActorDefinition(A_SOURCE_DEFINITION_ID, A_MANIFEST, adaptedConnectorSpecification);
    verify(configRepository, times(1)).insertActiveDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION.longValue())
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
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
        .withVersion(A_VERSION.longValue())
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(configRepository, times(0)).updateDeclarativeActorDefinition(any(), any(), any());
  }

  private void givenSourceDefinitionAvailableInWorkspace() throws IOException {
    when(configRepository.workspaceCanUseCustomDefinition(any(), any())).thenReturn(true);
  }

  private void givenSourceIsDeclarative() throws IOException {
    when(configRepository.getDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
        .thenReturn(Stream.of(new DeclarativeManifest().withVersion(ANOTHER_VERSION.longValue())));
  }

  private void setupConnectorSpecificationAdapter(final JsonNode spec, final String documentationUrl) {
    when(specAdapter.adapt(spec)).thenReturn(adaptedConnectorSpecification);
    when(adaptedConnectorSpecification.getDocumentationUrl()).thenReturn(URI.create(documentationUrl));
  }

  private static DeclarativeSourceManifest anyDeclarativeManifest() {
    return new DeclarativeSourceManifest().version(A_VERSION);
  }

}
