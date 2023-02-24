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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectPersistenceTest extends BaseConfigDatabaseTest {

  private ConfigRepository configRepository;
  private UUID mainWorkspace;

  private ConnectorBuilderProject project1;

  private ConnectorBuilderProject project2;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database);
  }

  @Test
  void testRead() throws IOException, ConfigNotFoundException {
    createBaseObjects();
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
        Arrays.asList(project1, project2)), configRepository.getConnectorBuilderProjectsByWorkspace(mainWorkspace).toList());
  }

  @Test
  void testUpdate() throws IOException, ConfigNotFoundException {
    createBaseObjects();
    project1.setName("Updated name");
    project1.setManifestDraft(new ObjectMapper().readTree("{}"));
    configRepository.writeBuilderProject(project1);
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

  private void createBaseObjects() throws IOException {
    mainWorkspace = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();

    project1 = createConnectorBuilderProject(mainWorkspace, false);
    project2 = createConnectorBuilderProject(mainWorkspace, false);

    // deleted project, should not show up in listing
    createConnectorBuilderProject(mainWorkspace, true);

    // unreachable project, should not show up in listing
    createConnectorBuilderProject(workspaceId2, false);
  }

  private ConnectorBuilderProject createConnectorBuilderProject(final UUID workspace, final boolean deleted)
      throws IOException {
    final UUID projectId = UUID.randomUUID();
    final ConnectorBuilderProject project = new ConnectorBuilderProject()
        .withBuilderProjectId(projectId)
        .withName("project " + projectId)
        .withTombstone(deleted)
        .withManifestDraft(new ObjectMapper().readTree("{\"the_id\": \"" + projectId + "\"}"))
        .withWorkspaceId(workspace);
    configRepository.writeBuilderProject(project);
    return project;
  }

}
