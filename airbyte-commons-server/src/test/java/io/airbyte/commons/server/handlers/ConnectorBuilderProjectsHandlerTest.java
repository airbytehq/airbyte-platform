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
import io.airbyte.api.model.generated.DeclarativeSourceManifest;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.init.CdkVersionProvider;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final Long A_VERSION = 32L;
  private static final Long ACTIVE_MANIFEST_VERSION = 865L;
  private static final String A_DESCRIPTION = "a description";
  private static final String A_SOURCE_NAME = "a source name";
  private static final String A_NAME = "a name";
  private static final String A_DOCUMENTATION_URL = "http://documentation.url";
  private static final JsonNode A_MANIFEST;
  private static final JsonNode A_SPEC;
  private static final ActorDefinitionConfigInjection A_CONFIG_INJECTION = new ActorDefinitionConfigInjection().withInjectionPath("something");
  private static final String CDK_VERSION = "8.9.10";

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
  private DeclarativeSourceManifestInjector manifestInjector;
  private CdkVersionProvider cdkVersionProvider;
  private ConnectorSpecification adaptedConnectorSpecification;
  private UUID workspaceId;
  private final String draftJsonString = "{\"test\":123,\"empty\":{\"array_in_object\":[]}}";

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws JsonProcessingException {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    manifestInjector = mock(DeclarativeSourceManifestInjector.class);
    cdkVersionProvider = mock(CdkVersionProvider.class);
    when(cdkVersionProvider.getCdkVersion()).thenReturn(CDK_VERSION);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);
    setupConnectorSpecificationAdapter(any(), "");
    workspaceId = UUID.randomUUID();

    connectorBuilderProjectsHandler = new ConnectorBuilderProjectsHandler(configRepository, cdkVersionProvider, uuidSupplier, manifestInjector);
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

    verify(configRepository, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), project.getManifestDraft());
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

    verify(configRepository, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), project.getManifestDraft());
  }

  @Test
  void givenActorDefinitionAssociatedWithProjectWhenUpdateConnectorBuilderProjectThenUpdateProjectAndDefinition() throws Exception {
    when(configRepository.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, false)).thenReturn(anyBuilderProject()
        .withBuilderProjectId(A_BUILDER_PROJECT_ID)
        .withWorkspaceId(A_WORKSPACE_ID)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID));

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(A_SOURCE_NAME)
            .draftManifest(A_MANIFEST))
        .workspaceId(A_WORKSPACE_ID)
        .builderProjectId(A_BUILDER_PROJECT_ID));

    verify(configRepository, times(1))
        .updateBuilderProjectAndActorDefinition(
            A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_SOURCE_NAME, A_MANIFEST, A_SOURCE_DEFINITION_ID);
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

    verify(configRepository, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), null);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should validate whether the workspace does not match")
  void testUpdateConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
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

    verify(configRepository, never()).writeBuilderProjectDraft(any(UUID.class), any(UUID.class), any(String.class), any(JsonNode.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  void testDeleteConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
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
  void testDeleteConnectorBuilderProject() throws IOException, ConfigNotFoundException {
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
    project1.setActiveDeclarativeManifestVersion(A_VERSION);
    project1.setActorDefinitionId(UUID.randomUUID());

    when(configRepository.getConnectorBuilderProjectsByWorkspace(workspaceId)).thenReturn(Stream.of(project1, project2));

    final ConnectorBuilderProjectReadList response =
        connectorBuilderProjectsHandler.listConnectorBuilderProjects(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(project1.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId());
    assertEquals(project2.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId());

    assertTrue(response.getProjects().get(0).getHasDraft());
    assertFalse(response.getProjects().get(1).getHasDraft());

    assertEquals(project1.getActiveDeclarativeManifestVersion(), response.getProjects().get(0).getActiveDeclarativeManifestVersion());
    assertEquals(project1.getActorDefinitionId(), response.getProjects().get(0).getSourceDefinitionId());
    Assertions.assertNull(project2.getActiveDeclarativeManifestVersion());
    Assertions.assertNull(project2.getActorDefinitionId());

    verify(configRepository, times(1))
        .getConnectorBuilderProjectsByWorkspace(
            workspaceId);
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  void testGetConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setActorDefinitionId(UUID.randomUUID());
    project.setActiveDeclarativeManifestVersion(A_VERSION);

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertEquals(project.getActorDefinitionId(), response.getBuilderProject().getSourceDefinitionId());
    assertEquals(project.getActiveDeclarativeManifestVersion(), response.getBuilderProject().getActiveDeclarativeManifestVersion());
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
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  void givenNoVersionButActiveManifestWhenGetConnectorBuilderProjectWithManifestThenReturnActiveVersion()
      throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject()
        .withManifestDraft(null)
        .withHasDraft(false)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID);
    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
    when(configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID)).thenReturn(new DeclarativeManifest()
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION));

    final ConnectorBuilderProjectRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertFalse(response.getBuilderProject().getHasDraft());
    assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion());
    assertEquals(A_MANIFEST, response.getDeclarativeManifest().getManifest());
    assertEquals(false, response.getDeclarativeManifest().getIsDraft());
    assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription());
  }

  @Test
  void givenVersionWhenGetConnectorBuilderProjectWithManifestThenReturnSpecificVersion() throws ConfigNotFoundException, IOException {
    when(configRepository.getConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(false))).thenReturn(
        new ConnectorBuilderProject().withWorkspaceId(A_WORKSPACE_ID));
    when(configRepository.getVersionedConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(A_VERSION))).thenReturn(
        new ConnectorBuilderProjectVersionedManifest()
            .withBuilderProjectId(A_BUILDER_PROJECT_ID)
            .withActiveDeclarativeManifestVersion(ACTIVE_MANIFEST_VERSION)
            .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .withHasDraft(true)
            .withName(A_NAME)
            .withManifest(A_MANIFEST)
            .withManifestVersion(A_VERSION)
            .withManifestDescription(A_DESCRIPTION));

    final ConnectorBuilderProjectRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(A_BUILDER_PROJECT_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION));

    assertEquals(A_BUILDER_PROJECT_ID, response.getBuilderProject().getBuilderProjectId());
    assertEquals(A_NAME, response.getBuilderProject().getName());
    assertEquals(ACTIVE_MANIFEST_VERSION, response.getBuilderProject().getActiveDeclarativeManifestVersion());
    assertEquals(A_SOURCE_DEFINITION_ID, response.getBuilderProject().getSourceDefinitionId());
    assertEquals(true, response.getBuilderProject().getHasDraft());
    assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion());
    assertEquals(A_MANIFEST, response.getDeclarativeManifest().getManifest());
    assertEquals(false, response.getDeclarativeManifest().getIsDraft());
    assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription());
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
    when(manifestInjector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)).thenReturn(A_CONFIG_INJECTION);
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().workspaceId(workspaceId).name(A_SOURCE_NAME)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC)));

    verify(manifestInjector, times(1)).addInjectedDeclarativeManifest(A_SPEC);
    verify(configRepository, times(1)).writeCustomConnectorMetadata(eq(new StandardSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_SOURCE_NAME)
        .withSourceType(SourceType.CUSTOM)
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)), eq(
            new ActorDefinitionVersion()
                .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
                .withDockerRepository("airbyte/source-declarative-manifest")
                .withDockerImageTag(CDK_VERSION)
                .withSpec(adaptedConnectorSpecification)
                .withSupportLevel(SupportLevel.NONE)
                .withReleaseStage(ReleaseStage.CUSTOM)
                .withDocumentationUrl(A_DOCUMENTATION_URL)
                .withProtocolVersion("0.2.0")),
        eq(workspaceId),
        eq(ScopeType.WORKSPACE));
    verify(configRepository, times(1)).writeActorDefinitionConfigInjectionForPath(eq(A_CONFIG_INJECTION));
  }

  @Test
  void whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).insertActiveDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(configRepository, times(1)).assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
  }

  @Test
  void whenPublishConnectorBuilderProjectThenDraftDeleted() throws IOException {
    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).deleteBuilderProjectDraft(A_BUILDER_PROJECT_ID);
  }

  private static ConnectorBuilderPublishRequestBody anyConnectorBuilderProjectRequest() {
    return new ConnectorBuilderPublishRequestBody().initialDeclarativeManifest(anyInitialManifest());
  }

  private static DeclarativeSourceManifest anyInitialManifest() {
    return new DeclarativeSourceManifest().version(A_VERSION);
  }

  private static ConnectorBuilderProject anyBuilderProject() {
    return new ConnectorBuilderProject();
  }

  private void setupConnectorSpecificationAdapter(final JsonNode spec, final String documentationUrl) {
    when(manifestInjector.createDeclarativeManifestConnectorSpecification(spec)).thenReturn(adaptedConnectorSpecification);
    when(adaptedConnectorSpecification.getDocumentationUrl()).thenReturn(URI.create(documentationUrl));
  }

}
