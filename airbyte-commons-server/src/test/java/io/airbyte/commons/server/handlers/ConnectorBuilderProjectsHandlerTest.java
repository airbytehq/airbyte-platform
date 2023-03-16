/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBodyInitialDeclarativeManifest;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.config.ActiveDeclarativeManifest;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final Integer A_VERSION = 32;
  private static final String A_DESCRIPTION = "a description";
  private static final String A_SOURCE_NAME = "a source name";
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
  private ConnectorBuilderProjectsHandler connectorBuilderProjectsHandler;
  private Supplier<UUID> uuidSupplier;
  private ConnectorBuilderSpecAdapter specAdapter;
  private ConnectorSpecification adaptedConnectorSpecification;
  private UUID workspaceId;
  private final String draftJsonString = "{\"test\":123,\"empty\":{\"array_in_object\":[]}}";

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws JsonProcessingException {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    specAdapter = mock(ConnectorBuilderSpecAdapter.class);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);
    setupConnectorSpecificationAdapter(any(), "");
    workspaceId = UUID.randomUUID();

    connectorBuilderProjectsHandler = new ConnectorBuilderProjectsHandler(configRepository, uuidSupplier, specAdapter);
  }

  private ConnectorBuilderProject generateBuilderProject() throws JsonProcessingException {
    final UUID projectId = UUID.randomUUID();
    return new ConnectorBuilderProject().withBuilderProjectId(projectId).withWorkspaceId(workspaceId).withName("Test project")
        .withHasDraft(true).withManifestDraft(new ObjectMapper().readTree(draftJsonString));
  }

  @Test
  @DisplayName("createConnectorBuilderProject should create a new project and return the id")
  void testCreateConnectorBuilderProject() throws IOException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(uuidSupplier.get()).thenReturn(project.getBuilderProjectId());

    final ConnectorBuilderProjectWithWorkspaceId create = new ConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId);

    final ConnectorBuilderProjectIdWithWorkspaceId response = connectorBuilderProjectsHandler.createConnectorBuilderProject(create);
    assertEquals(project.getBuilderProjectId(), response.getBuilderProjectId());
    assertEquals(project.getWorkspaceId(), response.getWorkspaceId());

    // hasDraft is not set when writing
    project.setHasDraft(null);
    verify(configRepository, times(1))
        .writeBuilderProject(
            project);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project")
  void testUpdateConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId());

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(update);

    // hasDraft is not set when writing
    project.setHasDraft(null);
    verify(configRepository, times(1))
        .writeBuilderProject(
            project);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project removing the draft")
  void testUpdateConnectorBuilderProjectWipeDraft() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails().name(project.getName()))
        .workspaceId(workspaceId).builderProjectId(project.getBuilderProjectId());

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(update);

    // validate project was written without manifest
    project.setManifestDraft(null);
    project.setHasDraft(null);
    verify(configRepository, times(1))
        .writeBuilderProject(
            project);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should validate whether the workspace does not match")
  void testUpdateConnectorBuilderProjectValidateWorkspace() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final UUID wrongWorkspace = UUID.randomUUID();

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId());

    project.setWorkspaceId(wrongWorkspace);
    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.updateConnectorBuilderProject(update));

    verify(configRepository, never()).writeBuilderProject(any(ConnectorBuilderProject.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  void testDeleteConnectorBuilderProjectValidateWorkspace() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final UUID wrongWorkspace = UUID.randomUUID();

    project.setWorkspaceId(wrongWorkspace);
    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId)));

    verify(configRepository, never()).deleteBuilderProject(any(UUID.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should delete an existing project")
  void testDeleteConnectorBuilderProject() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    verify(configRepository, times(1))
        .deleteBuilderProject(
            project.getBuilderProjectId());
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list all projects without drafts")
  void testListConnectorBuilderProject() throws IOException {
    final ConnectorBuilderProject project1 = generateBuilderProject();
    final ConnectorBuilderProject project2 = generateBuilderProject();
    project2.setHasDraft(false);

    when(configRepository.getConnectorBuilderProjectsByWorkspace(workspaceId)).thenReturn(Stream.of(project1, project2));

    final ConnectorBuilderProjectReadList response =
        connectorBuilderProjectsHandler.listConnectorBuilderProjects(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(project1.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId());
    assertEquals(project2.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId());

    assertTrue(response.getProjects().get(0).getHasDraft());
    assertFalse(response.getProjects().get(1).getHasDraft());

    verify(configRepository, times(1))
        .getConnectorBuilderProjectsByWorkspace(
            workspaceId);
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  void testGetConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertTrue(response.getDeclarativeManifest().getIsDraft());
    assertEquals(draftJsonString, new ObjectMapper().writeValueAsString(response.getDeclarativeManifest().getManifest()));
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  void testGetConnectorBuilderProjectWithoutDraft() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setManifestDraft(null);
    project.setHasDraft(false);

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertFalse(response.getBuilderProject().getHasDraft());
    assertNull(response.getDeclarativeManifest());
  }

  @Test
  void whenPublishConnectorBuilderProjectThenReturnActorDefinition() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    final SourceDefinitionIdBody response = connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest());
    assertEquals(A_SOURCE_DEFINITION_ID, response.getSourceDefinitionId());
  }

  @Test
  void whenPublishConnectorBuilderProjectThenCreateActorDefinition() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().workspaceId(workspaceId).name(A_SOURCE_NAME)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC)));

    verify(configRepository, times(1)).writeCustomSourceDefinition(eq(new StandardSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withDockerImageTag("0.29.0")
        .withDockerRepository("airbyte/source-declarative-manifest")
        .withName(A_SOURCE_NAME)
        .withProtocolVersion("0.2.0")
        .withSourceType(SourceType.CUSTOM)
        .withSpec(adaptedConnectorSpecification)
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)
        .withReleaseStage(ReleaseStage.CUSTOM)
        .withDocumentationUrl(A_DOCUMENTATION_URL)), eq(workspaceId));
    verify(configRepository, times(1))
        .writeActorDefinitionConfigInjectionForPath(eq(new ActorDefinitionConfigInjection().withActorDefinitionId(A_SOURCE_DEFINITION_ID)
            .withInjectionPath("__injected_declarative_manifest").withJsonToInject(A_MANIFEST)));
  }

  @Test
  void whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).insertDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION.longValue())
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(configRepository, times(1)).upsertActiveDeclarativeManifest(
        new ActiveDeclarativeManifest().withVersion(A_VERSION.longValue()).withActorDefinitionId(A_SOURCE_DEFINITION_ID));
    verify(configRepository, times(1)).assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
  }

  private static ConnectorBuilderPublishRequestBody anyConnectorBuilderProjectRequest() {
    return new ConnectorBuilderPublishRequestBody().initialDeclarativeManifest(anyInitialManifest());
  }

  private static ConnectorBuilderPublishRequestBodyInitialDeclarativeManifest anyInitialManifest() {
    return new ConnectorBuilderPublishRequestBodyInitialDeclarativeManifest().version(A_VERSION);
  }

  private void setupConnectorSpecificationAdapter(final JsonNode spec, final String documentationUrl) {
    when(specAdapter.adapt(spec)).thenReturn(adaptedConnectorSpecification);
    when(adaptedConnectorSpecification.getDocumentationUrl()).thenReturn(URI.create(documentationUrl));
  }

}
