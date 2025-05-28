/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.Jsons;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
  private static final String A_COMPONENTS_FILE_CONTENT = "a = 1";
  private static final JsonNode ANOTHER_MANIFEST;
  private static final String UPDATED_AT = "updatedAt";

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      ANOTHER_MANIFEST = new ObjectMapper().readTree("{\"another_manifest\": \"another_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private SourceService sourceService;
  private WorkspaceService workspaceService;
  private ConnectorBuilderService connectorBuilderService;
  private UUID mainWorkspace;

  private ConnectorBuilderProject project1;

  private ConnectorBuilderProject project2;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    final ConnectionService connectionService = mock(ConnectionService.class);
    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    final MetricClient metricClient = mock(MetricClient.class);

    organizationService.writeOrganization(MockData.defaultOrganization());
    final DataplaneGroupService dataplaneGroupService = new DataplaneGroupServiceTestJooqImpl(database);
    dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false));
    sourceService = new SourceServiceJooqImpl(database, featureFlagClient,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater, metricClient);
    workspaceService = new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter,
        secretPersistenceConfigService, metricClient);
    connectorBuilderService = new ConnectorBuilderServiceJooqImpl(database);
  }

  @Test
  void testRead() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true);
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(project1)
        .usingRecursiveComparison()
        .ignoringFields(UPDATED_AT)
        .isEqualTo(project);
    assertNotNull(project.getUpdatedAt());
  }

  @Test
  void testReadWithoutManifest() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    project1.setManifestDraft(null);
    ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), false);
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(project1)
        .usingRecursiveComparison()
        .ignoringFields(UPDATED_AT)
        .isEqualTo(project);
    assertNotNull(project.getUpdatedAt());
  }

  @Test
  void testReadWithLinkedDefinition() throws IOException, ConfigNotFoundException {
    createBaseObjects();

    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());

    // project 1 should be associated with the newly created source definition
    project1.setActorDefinitionId(sourceDefinition.getSourceDefinitionId());
    project1.setActiveDeclarativeManifestVersion(MANIFEST_VERSION);

    ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true);
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(project1)
        .usingRecursiveComparison()
        .ignoringFields(UPDATED_AT)
        .isEqualTo(project);
    assertNotNull(project.getUpdatedAt());
  }

  @Test
  void testReadNotExists() {
    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderService.getConnectorBuilderProject(UUID.randomUUID(), false));
  }

  @Test
  void testList() throws IOException {
    createBaseObjects();

    // set draft to null because it won't be returned as part of listing call
    project1.setManifestDraft(null);
    project2.setManifestDraft(null);

    List<ConnectorBuilderProject> projects = connectorBuilderService.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList();

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(
        new ArrayList<>(
            // project2 comes first due to alphabetical ordering
            Arrays.asList(project2, project1)))
                .usingRecursiveComparison()
                .ignoringFields(UPDATED_AT)
                .isEqualTo(projects);
    assertNotNull(projects.get(0).getUpdatedAt());
    assertNotNull(projects.get(1).getUpdatedAt());
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

    List<ConnectorBuilderProject> projects = connectorBuilderService.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList();

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(
        new ArrayList<>(
            // project2 comes first due to alphabetical ordering
            Arrays.asList(project2, project1)))
                .usingRecursiveComparison()
                .ignoringFields(UPDATED_AT)
                .isEqualTo(projects);
    assertNotNull(projects.get(0).getUpdatedAt());
    assertNotNull(projects.get(1).getUpdatedAt());
  }

  @Test
  void testListWithNoManifest() throws IOException {
    createBaseObjects();

    // actually set draft to null for first project
    project1.setManifestDraft(null);
    project1.setHasDraft(false);
    connectorBuilderService.writeBuilderProjectDraft(
        project1.getBuilderProjectId(),
        project1.getWorkspaceId(),
        project1.getName(),
        null,
        null,
        project1.getBaseActorDefinitionVersionId(),
        project1.getContributionPullRequestUrl(),
        project1.getContributionActorDefinitionId());

    // set draft to null because it won't be returned as part of listing call
    project2.setManifestDraft(null);
    // has draft is still truthy because there is a draft in the database
    project2.setHasDraft(true);

    List<ConnectorBuilderProject> projects = connectorBuilderService.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList();

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(
        new ArrayList<>(
            // project2 comes first due to alphabetical ordering
            Arrays.asList(project2, project1)))
                .usingRecursiveComparison()
                .ignoringFields(UPDATED_AT)
                .isEqualTo(projects);
    assertNotNull(projects.get(0).getUpdatedAt());
    assertNotNull(projects.get(1).getUpdatedAt());
  }

  @Test
  void testUpdate() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    project1.setName("Updated name");
    project1.setManifestDraft(new ObjectMapper().readTree("{}"));
    connectorBuilderService.writeBuilderProjectDraft(project1.getBuilderProjectId(), project1.getWorkspaceId(), project1.getName(),
        project1.getManifestDraft(), project1.getComponentsFileContent(),
        project1.getBaseActorDefinitionVersionId(), project1.getContributionPullRequestUrl(),
        project1.getContributionActorDefinitionId());
    ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true);
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    assertThat(project1)
        .usingRecursiveComparison()
        .ignoringFields(UPDATED_AT)
        .isEqualTo(project);
    assertNotNull(project.getUpdatedAt());
  }

  @Test
  void whenUpdateBuilderProjectAndActorDefinitionThenUpdateConnectorBuilderAndActorDefinition() throws Exception {
    connectorBuilderService.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null);
    workspaceService.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(A_WORKSPACE_ID));
    sourceService.writeCustomConnectorMetadata(MockData.customSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(false),
        MockData.actorDefinitionVersion().withActorDefinitionId(A_SOURCE_DEFINITION_ID), A_WORKSPACE_ID, ScopeType.WORKSPACE);

    connectorBuilderService.updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, ANOTHER_PROJECT_NAME, ANOTHER_MANIFEST, null, null, null, null, A_SOURCE_DEFINITION_ID);

    final ConnectorBuilderProject updatedConnectorBuilder = connectorBuilderService.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, true);
    assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.getName());
    assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.getManifestDraft());
    assertEquals(ANOTHER_PROJECT_NAME, sourceService.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName());
  }

  @Test
  void givenSourceIsPublicWhenUpdateBuilderProjectAndActorDefinitionThenActorDefinitionNameIsNotUpdated() throws Exception {
    connectorBuilderService.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null);
    workspaceService.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(A_WORKSPACE_ID));
    sourceService.writeCustomConnectorMetadata(MockData.customSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(true),
        MockData.actorDefinitionVersion().withActorDefinitionId(A_SOURCE_DEFINITION_ID), A_WORKSPACE_ID, ScopeType.WORKSPACE);

    connectorBuilderService.updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, ANOTHER_PROJECT_NAME, ANOTHER_MANIFEST, null, null, null, null, A_SOURCE_DEFINITION_ID);

    assertEquals(A_PROJECT_NAME, sourceService.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName());
  }

  @Test
  void testUpdateWithComponentsFile() throws Exception {
    connectorBuilderService.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null);
    workspaceService.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(A_WORKSPACE_ID));
    sourceService.writeCustomConnectorMetadata(MockData.customSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(false),
        MockData.actorDefinitionVersion().withActorDefinitionId(A_SOURCE_DEFINITION_ID), A_WORKSPACE_ID, ScopeType.WORKSPACE);

    connectorBuilderService.updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, ANOTHER_PROJECT_NAME, ANOTHER_MANIFEST, A_COMPONENTS_FILE_CONTENT, null, null, null,
        A_SOURCE_DEFINITION_ID);

    final ConnectorBuilderProject updatedConnectorBuilder = connectorBuilderService.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, true);
    assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.getName());
    assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.getManifestDraft());
    assertEquals(A_COMPONENTS_FILE_CONTENT, updatedConnectorBuilder.getComponentsFileContent());
  }

  @Test
  void testDelete() throws IOException, ConfigNotFoundException {
    createBaseObjects();

    final boolean deleted = connectorBuilderService.deleteBuilderProject(project1.getBuilderProjectId());
    assertTrue(deleted);
    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), false));
    assertNotNull(connectorBuilderService.getConnectorBuilderProject(project2.getBuilderProjectId(), false));
  }

  @Test
  void testAssignActorDefinitionToConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject connectorBuilderProject = createConnectorBuilderProject(UUID.randomUUID(), "any", false);
    final UUID aNewActorDefinitionId = UUID.randomUUID();

    connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), aNewActorDefinitionId);

    assertEquals(aNewActorDefinitionId,
        connectorBuilderService.getConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), false).getActorDefinitionId());
  }

  @Test
  void givenProjectDoesNotExistWhenGetVersionedConnectorBuilderProjectThenThrowException() {
    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderService.getVersionedConnectorBuilderProject(UUID.randomUUID(), 1L));
  }

  @Test
  void givenNoMatchingActiveDeclarativeManifestWhenGetVersionedConnectorBuilderProjectThenThrowException() throws IOException {
    connectorBuilderService.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, ANY_UUID, A_PROJECT_NAME, new ObjectMapper().readTree("{}"), null, null,
        null,
        null);
    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderService.getVersionedConnectorBuilderProject(A_BUILDER_PROJECT_ID, 1L));
  }

  @Test
  void whenGetVersionedConnectorBuilderProjectThenReturnVersionedProject() throws ConfigNotFoundException, IOException {
    connectorBuilderService.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, ANY_UUID, A_PROJECT_NAME, new ObjectMapper().readTree("{}"), null, null,
        null,
        null);
    connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
    connectorBuilderService.insertActiveDeclarativeManifest(anyDeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withDescription(A_DESCRIPTION)
        .withVersion(MANIFEST_VERSION)
        .withManifest(A_MANIFEST));
    connectorBuilderService.insertActiveDeclarativeManifest(anyDeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(ACTIVE_MANIFEST_VERSION));

    final ConnectorBuilderProjectVersionedManifest versionedConnectorBuilderProject = connectorBuilderService.getVersionedConnectorBuilderProject(
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
    assertNotNull(connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
    connectorBuilderService.deleteBuilderProjectDraft(project1.getBuilderProjectId());
    assertNull(connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
  }

  @Test
  void testDeleteManifestDraftForActorDefinitionId() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());
    assertNotNull(connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
    connectorBuilderService.deleteManifestDraftForActorDefinition(sourceDefinition.getSourceDefinitionId(), project1.getWorkspaceId());
    assertNull(connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true).getManifestDraft());
  }

  @Test
  void testGetConnectorBuilderProjectIdByActorDefinitionId() throws IOException {
    createBaseObjects();
    final StandardSourceDefinition sourceDefinition = linkSourceDefinition(project1.getBuilderProjectId());
    assertEquals(Optional.of(project1.getBuilderProjectId()),
        connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(sourceDefinition.getSourceDefinitionId()));
  }

  @Test
  void testGetConnectorBuilderProjectIdByActorDefinitionIdWhenNoMatch() throws IOException {
    createBaseObjects();
    assertEquals(Optional.empty(), connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(UUID.randomUUID()));
  }

  @Test
  void testCreateForkedProject() throws IOException, ConfigNotFoundException {
    createBaseObjects();

    // Create ADV and StandardSourceDefinition for DB constraints
    final UUID forkedADVId = UUID.randomUUID();
    final UUID forkedSourceDefId = UUID.randomUUID();
    final ActorDefinitionVersion forkedADV = MockData.actorDefinitionVersion().withVersionId(forkedADVId).withActorDefinitionId(forkedSourceDefId);
    sourceService.writeConnectorMetadata(MockData.standardSourceDefinitions().get(0).withSourceDefinitionId(forkedSourceDefId), forkedADV, List.of());

    final ConnectorBuilderProject forkedProject = new ConnectorBuilderProject()
        .withBuilderProjectId(UUID.randomUUID())
        .withName("Forked from another source")
        .withTombstone(false)
        .withManifestDraft(new ObjectMapper().readTree("{\"the_id\": \"" + UUID.randomUUID() + "\"}"))
        .withHasDraft(true)
        .withWorkspaceId(mainWorkspace)
        .withBaseActorDefinitionVersionId(forkedADVId);

    connectorBuilderService.writeBuilderProjectDraft(forkedProject.getBuilderProjectId(), forkedProject.getWorkspaceId(), forkedProject.getName(),
        forkedProject.getManifestDraft(), project1.getComponentsFileContent(), forkedProject.getBaseActorDefinitionVersionId(),
        forkedProject.getContributionPullRequestUrl(),
        forkedProject.getContributionActorDefinitionId());

    final ConnectorBuilderProject project = connectorBuilderService.getConnectorBuilderProject(forkedProject.getBuilderProjectId(), false);
    assertEquals(forkedADVId, project.getBaseActorDefinitionVersionId());
  }

  @Test
  void testAddContributionInfo() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    final UUID contributionActorDefinitionId = UUID.randomUUID();
    final String contributionPullRequestUrl = "https://github.com/airbytehq/airbyte/pull/1234";

    project1.setContributionPullRequestUrl(contributionPullRequestUrl);
    project1.setContributionActorDefinitionId(contributionActorDefinitionId);

    connectorBuilderService.writeBuilderProjectDraft(project1.getBuilderProjectId(), project1.getWorkspaceId(), project1.getName(),
        project1.getManifestDraft(), project1.getComponentsFileContent(), project1.getBaseActorDefinitionVersionId(),
        project1.getContributionPullRequestUrl(),
        project1.getContributionActorDefinitionId());

    final ConnectorBuilderProject updatedProject = connectorBuilderService.getConnectorBuilderProject(project1.getBuilderProjectId(), true);
    assertEquals(contributionPullRequestUrl, updatedProject.getContributionPullRequestUrl());
    assertEquals(contributionActorDefinitionId, updatedProject.getContributionActorDefinitionId());
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
    connectorBuilderService.writeBuilderProjectDraft(project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(),
        project.getManifestDraft(), project.getComponentsFileContent(), project.getBaseActorDefinitionVersionId(),
        project.getContributionPullRequestUrl(),
        project.getContributionActorDefinitionId());
    if (deleted) {
      connectorBuilderService.deleteBuilderProject(project.getBuilderProjectId());
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
        .withInternalSupportLevel(100L)
        .withSpec(new ConnectorSpecification().withProtocolVersion("0.1.0"));

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, List.of());
    connectorBuilderService.insertActiveDeclarativeManifest(new DeclarativeManifest()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersion(MANIFEST_VERSION)
        .withDescription("").withManifest(Jsons.emptyObject()).withSpec(Jsons.emptyObject()));
    connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(projectId, sourceDefinition.getSourceDefinitionId());

    return sourceDefinition;
  }

}
