/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.CONNECTION_SPECIFICATION_FIELD;
import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.SPEC_FIELD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ConfigRepositoryBuilderProjectUpdaterTest {

  private final JsonNode draftManifest = addSpec(Jsons.deserialize("{\"test\":123,\"empty\":{\"array_in_object\":[]}}"));

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final String A_DESCRIPTION = "a description";
  private static final String A_SOURCE_NAME = "a source name";
  private static final String A_NAME = "a name";
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

  private final String specString =
      """
      {
        "type": "object",
        "properties": {
          "username": {
            "type": "string"
          },
          "password": {
            "type": "string",
            "airbyte_secret": true
          }
        }
      }""";

  private ConfigRepository configRepository;
  private UUID workspaceId;
  private ConfigRepositoryBuilderProjectUpdater projectUpdater;

  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    projectUpdater = new ConfigRepositoryBuilderProjectUpdater(configRepository);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project removing the draft")
  void testUpdateConnectorBuilderProjectWipeDraft() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails().name(project.getName()))
        .workspaceId(workspaceId).builderProjectId(project.getBuilderProjectId());

    projectUpdater.persistBuilderProjectUpdate(update);

    verify(configRepository, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), null);
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

    projectUpdater.persistBuilderProjectUpdate(update);

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

    projectUpdater.persistBuilderProjectUpdate(new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(A_SOURCE_NAME)
            .draftManifest(A_MANIFEST))
        .workspaceId(A_WORKSPACE_ID)
        .builderProjectId(A_BUILDER_PROJECT_ID));

    verify(configRepository, times(1))
        .updateBuilderProjectAndActorDefinition(
            A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_SOURCE_NAME, A_MANIFEST, A_SOURCE_DEFINITION_ID);
  }

  private ConnectorBuilderProject generateBuilderProject() throws JsonProcessingException {
    final UUID projectId = UUID.randomUUID();
    return new ConnectorBuilderProject().withBuilderProjectId(projectId).withWorkspaceId(workspaceId).withName("Test project")
        .withHasDraft(true).withManifestDraft(draftManifest);
  }

  private JsonNode addSpec(JsonNode manifest) {
    final JsonNode spec = Jsons.deserialize("{\"" + CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}");
    return ((ObjectNode) Jsons.clone(manifest)).set(SPEC_FIELD, spec);
  }

  private static ConnectorBuilderProject anyBuilderProject() {
    return new ConnectorBuilderProject();
  }

}
