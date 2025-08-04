/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.UUID

internal class ConfigRepositoryBuilderProjectUpdaterTest {
  private val draftManifest: JsonNode? = addSpec(deserialize("{\"test\":123,\"empty\":{\"array_in_object\":[]}}"))

  private val specString =
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
    }
    """.trimIndent()

  private lateinit var connectorBuilderService: ConnectorBuilderService
  private lateinit var projectUpdater: ConfigRepositoryBuilderProjectUpdater

  @BeforeEach
  fun setUp() {
    connectorBuilderService = Mockito.mock(ConnectorBuilderService::class.java)
    projectUpdater = ConfigRepositoryBuilderProjectUpdater(connectorBuilderService)
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project removing the draft")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdateConnectorBuilderProjectWipeDraft() {
    val project = generateBuilderProject()

    Mockito
      .`when`<ConnectorBuilderProject?>(connectorBuilderService!!.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(ConnectorBuilderProjectDetails().name(project.getName()))
        .workspaceId(A_WORKSPACE_ID)
        .builderProjectId(project.getBuilderProjectId())

    projectUpdater!!.persistBuilderProjectUpdate(update)

    Mockito
      .verify<ConnectorBuilderService?>(connectorBuilderService, Mockito.times(1))
      .writeBuilderProjectDraft(
        project.getBuilderProjectId(),
        project.getWorkspaceId(),
        project.getName(),
        null,
        project.getComponentsFileContent(),
        project.getBaseActorDefinitionVersionId(),
        project.getContributionPullRequestUrl(),
        project.getContributionActorDefinitionId(),
      )
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdateConnectorBuilderProject() {
    val project = generateBuilderProject()

    Mockito
      .`when`<ConnectorBuilderProject?>(connectorBuilderService!!.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(
          ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()),
        ).workspaceId(A_WORKSPACE_ID)
        .builderProjectId(project.getBuilderProjectId())

    projectUpdater!!.persistBuilderProjectUpdate(update)

    Mockito
      .verify<ConnectorBuilderService?>(connectorBuilderService, Mockito.times(1))
      .writeBuilderProjectDraft(
        project.getBuilderProjectId(),
        project.getWorkspaceId(),
        project.getName(),
        project.getManifestDraft(),
        project.getComponentsFileContent(),
        project.getBaseActorDefinitionVersionId(),
        project.getContributionPullRequestUrl(),
        project.getContributionActorDefinitionId(),
      )
  }

  @Test
  @Throws(Exception::class)
  fun givenActorDefinitionAssociatedWithProjectWhenUpdateConnectorBuilderProjectThenUpdateProjectAndDefinition() {
    Mockito.`when`<ConnectorBuilderProject?>(connectorBuilderService!!.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, false)).thenReturn(
      anyBuilderProject()
        .withBuilderProjectId(A_BUILDER_PROJECT_ID)
        .withWorkspaceId(A_WORKSPACE_ID)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withBaseActorDefinitionVersionId(A_BASE_ACTOR_DEFINITION_VERSION_ID)
        .withContributionPullRequestUrl(A_CONTRIBUTION_PULL_REQUEST_URL)
        .withContributionActorDefinitionId(A_CONTRIBUTION_ACTOR_DEFINITION_ID),
    )

    projectUpdater!!.persistBuilderProjectUpdate(
      ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(
          ConnectorBuilderProjectDetails()
            .name(A_SOURCE_NAME)
            .draftManifest(A_MANIFEST)
            .baseActorDefinitionVersionId(A_BASE_ACTOR_DEFINITION_VERSION_ID)
            .contributionPullRequestUrl(A_CONTRIBUTION_PULL_REQUEST_URL)
            .contributionActorDefinitionId(A_CONTRIBUTION_ACTOR_DEFINITION_ID),
        ).workspaceId(A_WORKSPACE_ID)
        .builderProjectId(A_BUILDER_PROJECT_ID),
    )

    Mockito
      .verify<ConnectorBuilderService?>(connectorBuilderService, Mockito.times(1))
      .updateBuilderProjectAndActorDefinition(
        A_BUILDER_PROJECT_ID,
        A_WORKSPACE_ID,
        A_SOURCE_NAME,
        A_MANIFEST,
        null,
        A_BASE_ACTOR_DEFINITION_VERSION_ID,
        A_CONTRIBUTION_PULL_REQUEST_URL,
        A_CONTRIBUTION_ACTOR_DEFINITION_ID,
        A_SOURCE_DEFINITION_ID,
      )
  }

  @Throws(JsonProcessingException::class)
  private fun generateBuilderProject(): ConnectorBuilderProject {
    val projectId = UUID.randomUUID()
    return ConnectorBuilderProject()
      .withBuilderProjectId(projectId)
      .withWorkspaceId(A_WORKSPACE_ID)
      .withName("Test project")
      .withHasDraft(true)
      .withManifestDraft(draftManifest)
  }

  private fun addSpec(manifest: JsonNode): JsonNode? {
    val spec = deserialize("{\"" + ConnectorBuilderProjectsHandler.Companion.CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}")
    return (clone<JsonNode>(manifest) as ObjectNode).set<JsonNode?>(ConnectorBuilderProjectsHandler.Companion.SPEC_FIELD, spec)
  }

  companion object {
    private val A_SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val A_BUILDER_PROJECT_ID: UUID = UUID.randomUUID()
    private val A_WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val A_SOURCE_NAME = "a source name"
    private val A_MANIFEST: JsonNode?
    private val A_BASE_ACTOR_DEFINITION_VERSION_ID: UUID = UUID.randomUUID()
    private const val A_CONTRIBUTION_PULL_REQUEST_URL = "https://github.com/airbytehq/airbyte/pull/1234"
    private val A_CONTRIBUTION_ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()

    init {
      try {
        A_MANIFEST = ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }

    private fun anyBuilderProject(): ConnectorBuilderProject = ConnectorBuilderProject()
  }
}
