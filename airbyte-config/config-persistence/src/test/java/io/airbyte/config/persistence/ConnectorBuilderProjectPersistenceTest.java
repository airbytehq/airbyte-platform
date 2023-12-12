/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Jsons;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectPersistenceTest extends BaseConfigDatabaseTest {

  private static final Long MANIFEST_VERSION = 123L;
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final String A_PROJECT_NAME = "a project name";
  private static final String ANOTHER_PROJECT_NAME = "another project name";
  private static final UUID ANY_UUID = UUID.randomUUID();
  private static final String A_DESCRIPTION = "a description";
  private static final Long ACTIVE_MANIFEST_VERSION = 305L;
  private static final JsonNode A_MANIFEST;
  private static final JsonNode ANOTHER_MANIFEST;

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      ANOTHER_MANIFEST = new ObjectMapper().readTree("{\"another_manifest\": \"another_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigRepository configRepository;
  private UUID mainWorkspace;

  private ConnectorBuilderProject project1;

  private ConnectorBuilderProject project2;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));
  }

  @Test
  void testRead() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    assertEquals(project1, configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true));
  }

  @Test
  void testReadWithoutManifest() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    project1.setManifestDraft(null);
    assertEquals(project1, configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), false));
  }

  @Test
  void testReadWithLinkedDefinition() throws IOException, ConfigNotFoundException {
    createBaseObjects();

    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());

    // project 1 should be associated with the newly created source definition
    project1.setActorDefinitionId(sourceDefinition.getSourceDefinitionId());
    project1.setActiveDeclarativeManifestVersion(MANIFEST_VERSION);

    assertEquals(project1, configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true));
  }

  @Test
  void testReadNotExists() {
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getConnectorBuilderProject(UUID.randomUUID(), false));
  }

  @Test
  void testList() throws IOException {
    createBaseObjects();

    // set draft to null because it won't be returned as part of listing call
    project1.setManifestDraft(null);
    project2.setManifestDraft(null);

    assertEquals(new ArrayList<>(
        // project2 comes first due to alphabetical ordering
        Arrays.asList(project2, project1)), configRepository.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList());
  }

  @Test
  void testListWithLinkedDefinition() throws IOException {
    createBaseObjects();

    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());

    // set draft to null because it won't be returned as part of listing call
    project1.setManifestDraft(null);
    project2.setManifestDraft(null);

    // project 1 should be associated with the newly created source definition
    project1.setActiveDeclarativeManifestVersion(MANIFEST_VERSION);
    project1.setActorDefinitionId(sourceDefinition.getSourceDefinitionId());

    assertEquals(new ArrayList<>(
        // project2 comes first due to alphabetical ordering
        Arrays.asList(project2, project1)), configRepository.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList());
  }

  @Test
  void testListWithNoManifest() throws IOException {
    createBaseObjects();

    // actually set draft to null for first project
    project1.setManifestDraft(null);
    project1.setHasDraft(false);
    configRepository.writeBuilderProjectDraft(project1.getBuilderProjectId(), project1.getWorkspaceId(), project1.getName(), null);

    // set draft to null because it won't be returned as part of listing call
    project2.setManifestDraft(null);
    // has draft is still truthy because there is a draft in the database
    project2.setHasDraft(true);

    assertEquals(new ArrayList<>(
        // project2 comes first due to alphabetical ordering
        Arrays.asList(project2, project1)), configRepository.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList());
  }

  @Test
  void testUpdate() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    project1.setName("Updated name");
    project1.setManifestDraft(new ObjectMapper().readTree("{}"));
    configRepository.writeBuilderProjectDraft(project1.getBuilderProjectId(), project1.getWorkspaceId(), project1.getName(),
        project1.getManifestDraft());
    assertEquals(project1, configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true));
  }

  @Test
  void whenUpdateBuilderProjectAndActorDefinitionThenUpdateConnectorBuilderAndActorDefinition() throws Exception {
    configRepository.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST);
    configRepository.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(A_WORKSPACE_ID));
    configRepository.writeCustomConnectorMetadata(MockData.customSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(false),
        MockData.actorDefinitionVersion().withActorDefinitionId(A_SOURCE_DEFINITION_ID), A_WORKSPACE_ID, ScopeType.WORKSPACE);

    configRepository.updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, ANOTHER_PROJECT_NAME, ANOTHER_MANIFEST, A_SOURCE_DEFINITION_ID);

    final ConnectorBuilderProject updatedConnectorBuilder = configRepository.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, true);
    assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.getName());
    assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.getManifestDraft());
    assertEquals(ANOTHER_PROJECT_NAME, configRepository.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName());
  }

  @Test
  void givenSourceIsPublicWhenUpdateBuilderProjectAndActorDefinitionThenActorDefinitionNameIsNotUpdated() throws Exception {
    configRepository.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST);
    configRepository.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(A_WORKSPACE_ID));
    configRepository.writeCustomConnectorMetadata(MockData.customSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(true),
        MockData.actorDefinitionVersion().withActorDefinitionId(A_SOURCE_DEFINITION_ID), A_WORKSPACE_ID, ScopeType.WORKSPACE);

    configRepository.updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, ANOTHER_PROJECT_NAME, ANOTHER_MANIFEST, A_SOURCE_DEFINITION_ID);

    assertEquals(A_PROJECT_NAME, configRepository.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName());
  }

  @Test
  void testDelete() throws IOException, ConfigNotFoundException {
    createBaseObjects();

    final boolean deleted = configRepository.deleteBuilderProject(project1.getBuilderProjectId());
    assertTrue(deleted);
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), false));
    assertNotNull(configRepository.getConnectorBuilderProject(project2.getBuilderProjectId(), false));
  }

  @Test
  void testAssignActorDefinitionToConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject connectorBuilderProject = createConnectorBuilderProject(UUID.randomUUID(), "any", false);
    final UUID aNewActorDefinitionId = UUID.randomUUID();

    configRepository.assignActorDefinitionToConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), aNewActorDefinitionId);

    assertEquals(aNewActorDefinitionId,
        configRepository.getConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), false).getActorDefinitionId());
  }

  @Test
  void givenProjectDoesNotExistWhenGetVersionedConnectorBuilderProjectThenThrowException() {
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getVersionedConnectorBuilderProject(UUID.randomUUID(), 1L));
  }

  @Test
  void givenNoMatchingActiveDeclarativeManifestWhenGetVersionedConnectorBuilderProjectThenThrowException() throws IOException {
    configRepository.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, ANY_UUID, A_PROJECT_NAME, new ObjectMapper().readTree("{}"));
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getVersionedConnectorBuilderProject(A_BUILDER_PROJECT_ID, 1L));
  }

  @Test
  void whenGetVersionedConnectorBuilderProjectThenReturnVersionedProject() throws ConfigNotFoundException, IOException {
    configRepository.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, ANY_UUID, A_PROJECT_NAME, new ObjectMapper().readTree("{}"));
    configRepository.assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
    configRepository.insertActiveDeclarativeManifest(anyDeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withDescription(A_DESCRIPTION)
        .withVersion(MANIFEST_VERSION)
        .withManifest(A_MANIFEST));
    configRepository.insertActiveDeclarativeManifest(anyDeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(ACTIVE_MANIFEST_VERSION));

    final ConnectorBuilderProjectVersionedManifest versionedConnectorBuilderProject = configRepository.getVersionedConnectorBuilderProject(
        A_BUILDER_PROJECT_ID, MANIFEST_VERSION);

    assertEquals(A_PROJECT_NAME, versionedConnectorBuilderProject.getName());
    assertEquals(A_BUILDER_PROJECT_ID, versionedConnectorBuilderProject.getBuilderProjectId());
    assertEquals(A_SOURCE_DEFINITION_ID, versionedConnectorBuilderProject.getSourceDefinitionId());
    assertEquals(ACTIVE_MANIFEST_VERSION, versionedConnectorBuilderProject.getActiveDeclarativeManifestVersion());
    assertEquals(MANIFEST_VERSION, versionedConnectorBuilderProject.getManifestVersion());
    assertEquals(A_DESCRIPTION, versionedConnectorBuilderProject.getManifestDescription());
    assertEquals(A_MANIFEST, versionedConnectorBuilderProject.getManifest());
  }

  @Test
  void testDeleteBuilderProjectDraft() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    assertNotNull(configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
    configRepository.deleteBuilderProjectDraft(project1.getBuilderProjectId());
    assertNull(configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
  }

  @Test
  void testDeleteManifestDraftForActorDefinitionId() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());
    assertNotNull(configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
    configRepository.deleteManifestDraftForActorDefinition(sourceDefinition.getSourceDefinitionId(), project1.getWorkspaceId());
    assertNull(configRepository.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
  }

  private DeclarativeManifest anyDeclarativeManifest() {
    try {
      return new DeclarativeManifest()
          .withActorDefinitionId(UUID.randomUUID())
          .withVersion(589345L)
          .withDescription("description for anyDeclarativeManifest")
          .withManifest(new ObjectMapper().readTree("{}"))
          .withSpec(new ObjectMapper().readTree("{}"));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void createBaseObjects() throws IOException {
    mainWorkspace = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();

    project1 = createConnectorBuilderProject(mainWorkspace, "Z project", false);
    project2 = createConnectorBuilderProject(mainWorkspace, "A project", false);

    // deleted project, should not show up in listing
    createConnectorBuilderProject(mainWorkspace, "Deleted project", true);

    // unreachable project, should not show up in listing
    createConnectorBuilderProject(workspaceId2, "Other workspace project", false);
  }

  private ConnectorBuilderProject createConnectorBuilderProject(final UUID workspace, final String name, final boolean deleted)
      throws IOException {
    final UUID projectId = UUID.randomUUID();
    final ConnectorBuilderProject project = new ConnectorBuilderProject()
        .withBuilderProjectId(projectId)
        .withName(name)
        .withTombstone(deleted)
        .withManifestDraft(new ObjectMapper().readTree("{\"the_id\": \"" + projectId + "\"}"))
        .withHasDraft(true)
        .withWorkspaceId(workspace);
    configRepository.writeBuilderProjectDraft(project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), project.getManifestDraft());
    if (deleted) {
      configRepository.deleteBuilderProject(project.getBuilderProjectId());
    }
    return project;
  }

  private StandardSourceDefinition linkSourceDefinition(final UUID projectId) throws IOException {
    final UUID id = UUID.randomUUID();

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withDockerRepository("repo-" + id)
        .withDockerImageTag("0.0.1")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withSpec(new ConnectorSpecification().withProtocolVersion("0.1.0"));

    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion);
    configRepository.insertActiveDeclarativeManifest(new DeclarativeManifest()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersion(MANIFEST_VERSION)
        .withDescription("").withManifest(Jsons.emptyObject()).withSpec(Jsons.emptyObject()));
    configRepository.assignActorDefinitionToConnectorBuilderProject(projectId, sourceDefinition.getSourceDefinitionId());

    return sourceDefinition;
  }

}
