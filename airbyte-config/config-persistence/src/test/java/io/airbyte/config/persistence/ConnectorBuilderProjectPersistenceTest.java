/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.protocol.models.Jsons;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectPersistenceTest extends BaseConfigDatabaseTest {

  private static final Long MANIFEST_VERSION = 123L;
  private ConfigRepository configRepository;
  private UUID mainWorkspace;

  private ConnectorBuilderProject project1;

  private ConnectorBuilderProject project2;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database, MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
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
  void testReadWithLinkedDefinition() throws IOException, ConfigNotFoundException, JsonValidationException {
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
  void testListWithLinkedDefinition() throws IOException, JsonValidationException {
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
  void testListWithNoManifest() throws IOException, ConfigNotFoundException {
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

  private StandardSourceDefinition linkSourceDefinition(final UUID projectId) throws JsonValidationException, IOException {
    final UUID id = UUID.randomUUID();

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withDockerRepository("source-image-" + id)
        .withDockerImageTag("0.0.1")
        .withSourceDefinitionId(id)
        .withProtocolVersion("0.2.0")
        .withTombstone(false);

    configRepository.writeStandardSourceDefinition(sourceDefinition);
    configRepository.insertActiveDeclarativeManifest(new DeclarativeManifest()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersion(MANIFEST_VERSION)
        .withDescription("").withManifest(Jsons.emptyObject()).withSpec(Jsons.emptyObject()));
    configRepository.assignActorDefinitionToConnectorBuilderProject(projectId, sourceDefinition.getSourceDefinitionId());

    return sourceDefinition;
  }

}
