/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.assertj.core.api.Assertions
import org.junit.Assert
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.util.Optional
import java.util.UUID
import org.mockito.Mockito.`when` as whenever

internal class ConnectorBuilderProjectPersistenceTest : BaseConfigDatabaseTest() {
  private var sourceService: SourceService? = null
  private var workspaceService: WorkspaceService? = null
  private var connectorBuilderService: ConnectorBuilderService? = null
  private var mainWorkspace: UUID? = null

  private var project1: ConnectorBuilderProject? = null

  private var project2: ConnectorBuilderProject? = null

  @BeforeEach
  fun beforeEach() {
    truncateAllTables()

    val featureFlagClient = mock(TestClient::class.java)
    whenever(
      featureFlagClient.stringVariation(org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages), org.mockito.kotlin.any<SourceDefinition>()),
    ).thenReturn("3600")

    val secretsRepositoryReader = mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = mock(SecretPersistenceConfigService::class.java)

    val connectionService = mock(ConnectionService::class.java)
    val organizationService = OrganizationServiceJooqImpl(database!!)
    val actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater::class.java)
    val metricClient = mock(MetricClient::class.java)
    val actorPaginationServiceHelper = mock(ActorServicePaginationHelper::class.java)

    // Create organization first
    val defaultOrg = MockData.defaultOrganization()
    organizationService.writeOrganization(defaultOrg)

    // Create dataplane group using the same organization ID
    val dataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(defaultOrg.organizationId)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    sourceService =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )

    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )

    connectorBuilderService = ConnectorBuilderServiceJooqImpl(database)
  }

  @Test
  fun testRead() {
    createBaseObjects()
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.updatedAt)
  }

  @Test
  fun testReadWithoutManifest() {
    createBaseObjects()
    project1!!.manifestDraft = null
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, false)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.updatedAt)
  }

  @Test
  fun testReadWithLinkedDefinition() {
    createBaseObjects()

    val sourceDefinition = linkSourceDefinition(project1!!.builderProjectId)

    // project 1 should be associated with the newly created source definition
    project1!!.actorDefinitionId = sourceDefinition.sourceDefinitionId
    project1!!.activeDeclarativeManifestVersion = MANIFEST_VERSION

    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.updatedAt)
  }

  @Test
  fun testReadNotExists() {
    Assert.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      connectorBuilderService!!.getConnectorBuilderProject(
        UUID.randomUUID(),
        false,
      )
    }
  }

  @Test
  fun testList() {
    createBaseObjects()

    // set draft to null because it won't be returned as part of listing call
    project1!!.manifestDraft = null
    project2!!.manifestDraft = null

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList( // project2 comes first due to alphabetical ordering
          listOf(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[0]!!.updatedAt)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[1]!!.updatedAt)
  }

  @Test
  fun testListWithLinkedDefinition() {
    createBaseObjects()

    val sourceDefinition = linkSourceDefinition(project1!!.builderProjectId)

    // set draft to null because it won't be returned as part of listing call
    project1!!.manifestDraft = null
    project2!!.manifestDraft = null

    // project 1 should be associated with the newly created source definition
    project1!!.activeDeclarativeManifestVersion = MANIFEST_VERSION
    project1!!.actorDefinitionId = sourceDefinition.sourceDefinitionId

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList( // project2 comes first due to alphabetical ordering
          listOf(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[0]!!.updatedAt)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[1]!!.updatedAt)
  }

  @Test
  fun testListWithNoManifest() {
    createBaseObjects()

    // actually set draft to null for first project
    project1!!.manifestDraft = null
    project1!!.hasDraft = false
    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.builderProjectId,
      project1!!.workspaceId,
      project1!!.name,
      null,
      null,
      project1!!.baseActorDefinitionVersionId,
      project1!!.contributionPullRequestUrl,
      project1!!.contributionActorDefinitionId,
    )

    // set draft to null because it won't be returned as part of listing call
    project2!!.manifestDraft = null
    // has draft is still truthy because there is a draft in the database
    project2!!.hasDraft = true

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList( // project2 comes first due to alphabetical ordering
          listOf(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[0]!!.updatedAt)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects[1]!!.updatedAt)
  }

  @Test
  fun testUpdate() {
    createBaseObjects()
    project1!!.name = "Updated name"
    project1!!.manifestDraft = ObjectMapper().readTree("{}")
    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.builderProjectId,
      project1!!.workspaceId,
      project1!!.name,
      project1!!.manifestDraft,
      project1!!.componentsFileContent,
      project1!!.baseActorDefinitionVersionId,
      project1!!.contributionPullRequestUrl,
      project1!!.contributionActorDefinitionId,
    )
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.updatedAt)
  }

  @Test
  fun whenUpdateBuilderProjectAndActorDefinitionThenUpdateConnectorBuilderAndActorDefinition() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces()[0]!!.withWorkspaceId(A_WORKSPACE_ID))
    sourceService!!.writeCustomConnectorMetadata(
      MockData
        .customSourceDefinition()!!
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(false),
      MockData.actorDefinitionVersion()!!.withActorDefinitionId(A_SOURCE_DEFINITION_ID),
      A_WORKSPACE_ID,
      ScopeType.WORKSPACE,
    )

    connectorBuilderService!!.updateBuilderProjectAndActorDefinition(
      A_BUILDER_PROJECT_ID,
      A_WORKSPACE_ID,
      ANOTHER_PROJECT_NAME,
      ANOTHER_MANIFEST,
      null,
      null,
      null,
      null,
      A_SOURCE_DEFINITION_ID,
    )

    val updatedConnectorBuilder = connectorBuilderService!!.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, true)
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.name)
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.manifestDraft)
    org.junit.jupiter.api.Assertions.assertEquals(
      ANOTHER_PROJECT_NAME,
      sourceService!!.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).name,
    )
  }

  @Test
  fun givenSourceIsPublicWhenUpdateBuilderProjectAndActorDefinitionThenActorDefinitionNameIsNotUpdated() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces()[0]!!.withWorkspaceId(A_WORKSPACE_ID))
    sourceService!!.writeCustomConnectorMetadata(
      MockData
        .customSourceDefinition()!!
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(true),
      MockData.actorDefinitionVersion()!!.withActorDefinitionId(A_SOURCE_DEFINITION_ID),
      A_WORKSPACE_ID,
      ScopeType.WORKSPACE,
    )

    connectorBuilderService!!.updateBuilderProjectAndActorDefinition(
      A_BUILDER_PROJECT_ID,
      A_WORKSPACE_ID,
      ANOTHER_PROJECT_NAME,
      ANOTHER_MANIFEST,
      null,
      null,
      null,
      null,
      A_SOURCE_DEFINITION_ID,
    )

    org.junit.jupiter.api.Assertions
      .assertEquals(A_PROJECT_NAME, sourceService!!.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).name)
  }

  @Test
  fun testUpdateWithComponentsFile() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces()[0]!!.withWorkspaceId(A_WORKSPACE_ID))
    sourceService!!.writeCustomConnectorMetadata(
      MockData
        .customSourceDefinition()!!
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_PROJECT_NAME)
        .withPublic(false),
      MockData.actorDefinitionVersion()!!.withActorDefinitionId(A_SOURCE_DEFINITION_ID),
      A_WORKSPACE_ID,
      ScopeType.WORKSPACE,
    )

    connectorBuilderService!!.updateBuilderProjectAndActorDefinition(
      A_BUILDER_PROJECT_ID,
      A_WORKSPACE_ID,
      ANOTHER_PROJECT_NAME,
      ANOTHER_MANIFEST,
      A_COMPONENTS_FILE_CONTENT,
      null,
      null,
      null,
      A_SOURCE_DEFINITION_ID,
    )

    val updatedConnectorBuilder = connectorBuilderService!!.getConnectorBuilderProject(A_BUILDER_PROJECT_ID, true)
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.name)
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.manifestDraft)
    org.junit.jupiter.api.Assertions
      .assertEquals(A_COMPONENTS_FILE_CONTENT, updatedConnectorBuilder.componentsFileContent)
  }

  @Test
  fun testDelete() {
    createBaseObjects()

    val deleted = connectorBuilderService!!.deleteBuilderProject(project1!!.builderProjectId)
    org.junit.jupiter.api.Assertions
      .assertTrue(deleted)
    Assert.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      connectorBuilderService!!.getConnectorBuilderProject(
        project1!!.builderProjectId,
        false,
      )
    }
    org.junit.jupiter.api.Assertions
      .assertNotNull(connectorBuilderService!!.getConnectorBuilderProject(project2!!.builderProjectId, false))
  }

  @Test
  fun testAssignActorDefinitionToConnectorBuilderProject() {
    val connectorBuilderProject = createConnectorBuilderProject(UUID.randomUUID(), "any", false)
    val aNewActorDefinitionId = UUID.randomUUID()

    connectorBuilderService!!.assignActorDefinitionToConnectorBuilderProject(connectorBuilderProject.builderProjectId, aNewActorDefinitionId)

    org.junit.jupiter.api.Assertions.assertEquals(
      aNewActorDefinitionId,
      connectorBuilderService!!.getConnectorBuilderProject(connectorBuilderProject.builderProjectId, false).actorDefinitionId,
    )
  }

  @Test
  fun givenProjectDoesNotExistWhenGetVersionedConnectorBuilderProjectThenThrowException() {
    Assert.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      connectorBuilderService!!.getVersionedConnectorBuilderProject(
        UUID.randomUUID(),
        1L,
      )
    }
  }

  @Test
  fun givenNoMatchingActiveDeclarativeManifestWhenGetVersionedConnectorBuilderProjectThenThrowException() {
    connectorBuilderService!!.writeBuilderProjectDraft(
      A_BUILDER_PROJECT_ID,
      ANY_UUID,
      A_PROJECT_NAME,
      ObjectMapper().readTree("{}"),
      null,
      null,
      null,
      null,
    )
    Assert.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      connectorBuilderService!!.getVersionedConnectorBuilderProject(
        A_BUILDER_PROJECT_ID,
        1L,
      )
    }
  }

  @Test
  fun whenGetVersionedConnectorBuilderProjectThenReturnVersionedProject() {
    connectorBuilderService!!.writeBuilderProjectDraft(
      A_BUILDER_PROJECT_ID,
      ANY_UUID,
      A_PROJECT_NAME,
      ObjectMapper().readTree("{}"),
      null,
      null,
      null,
      null,
    )
    connectorBuilderService!!.assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID)
    connectorBuilderService!!.insertActiveDeclarativeManifest(
      anyDeclarativeManifest()!!
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withDescription(A_DESCRIPTION)
        .withVersion(MANIFEST_VERSION)
        .withManifest(A_MANIFEST),
    )
    connectorBuilderService!!.insertActiveDeclarativeManifest(
      anyDeclarativeManifest()!!
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(ACTIVE_MANIFEST_VERSION),
    )

    val versionedConnectorBuilderProject =
      connectorBuilderService!!.getVersionedConnectorBuilderProject(
        A_BUILDER_PROJECT_ID,
        MANIFEST_VERSION,
      )

    org.junit.jupiter.api.Assertions
      .assertEquals(A_PROJECT_NAME, versionedConnectorBuilderProject.name)
    org.junit.jupiter.api.Assertions
      .assertEquals(A_BUILDER_PROJECT_ID, versionedConnectorBuilderProject.builderProjectId)
    org.junit.jupiter.api.Assertions
      .assertEquals(A_SOURCE_DEFINITION_ID, versionedConnectorBuilderProject.sourceDefinitionId)
    org.junit.jupiter.api.Assertions
      .assertEquals(ACTIVE_MANIFEST_VERSION, versionedConnectorBuilderProject.activeDeclarativeManifestVersion)
    org.junit.jupiter.api.Assertions
      .assertEquals(MANIFEST_VERSION, versionedConnectorBuilderProject.manifestVersion)
    org.junit.jupiter.api.Assertions
      .assertEquals(A_DESCRIPTION, versionedConnectorBuilderProject.manifestDescription)
    org.junit.jupiter.api.Assertions
      .assertEquals(A_MANIFEST, versionedConnectorBuilderProject.manifest)
  }

  @Test
  fun testDeleteBuilderProjectDraft() {
    createBaseObjects()
    org.junit.jupiter.api.Assertions.assertNotNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true).manifestDraft,
    )
    connectorBuilderService!!.deleteBuilderProjectDraft(project1!!.builderProjectId)
    org.junit.jupiter.api.Assertions.assertNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true).manifestDraft,
    )
  }

  @Test
  fun testDeleteManifestDraftForActorDefinitionId() {
    createBaseObjects()
    val sourceDefinition = linkSourceDefinition(project1!!.builderProjectId)
    org.junit.jupiter.api.Assertions.assertNotNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true).manifestDraft,
    )
    connectorBuilderService!!.deleteManifestDraftForActorDefinition(sourceDefinition.sourceDefinitionId, project1!!.workspaceId)
    org.junit.jupiter.api.Assertions.assertNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true).manifestDraft,
    )
  }

  @Test
  fun testGetConnectorBuilderProjectIdByActorDefinitionId() {
    createBaseObjects()
    val sourceDefinition = linkSourceDefinition(project1!!.builderProjectId)
    org.junit.jupiter.api.Assertions.assertEquals(
      Optional.of(project1!!.builderProjectId),
      connectorBuilderService!!.getConnectorBuilderProjectIdForActorDefinitionId(sourceDefinition.sourceDefinitionId),
    )
  }

  @Test
  fun testGetConnectorBuilderProjectIdByActorDefinitionIdWhenNoMatch() {
    createBaseObjects()
    org.junit.jupiter.api.Assertions.assertEquals(
      Optional.empty<Any?>(),
      connectorBuilderService!!.getConnectorBuilderProjectIdForActorDefinitionId(
        UUID.randomUUID(),
      ),
    )
  }

  @Test
  fun testCreateForkedProject() {
    createBaseObjects()

    // Create ADV and StandardSourceDefinition for DB constraints
    val forkedADVId = UUID.randomUUID()
    val forkedSourceDefId = UUID.randomUUID()
    val forkedADV = MockData.actorDefinitionVersion()!!.withVersionId(forkedADVId).withActorDefinitionId(forkedSourceDefId)
    sourceService!!.writeConnectorMetadata(
      MockData.standardSourceDefinitions()[0]!!.withSourceDefinitionId(forkedSourceDefId),
      forkedADV,
      mutableListOf(),
    )

    val forkedProject =
      ConnectorBuilderProject()
        .withBuilderProjectId(UUID.randomUUID())
        .withName("Forked from another source")
        .withTombstone(false)
        .withManifestDraft(ObjectMapper().readTree("{\"the_id\": \"" + UUID.randomUUID() + "\"}"))
        .withHasDraft(true)
        .withWorkspaceId(mainWorkspace)
        .withBaseActorDefinitionVersionId(forkedADVId)

    connectorBuilderService!!.writeBuilderProjectDraft(
      forkedProject.builderProjectId,
      forkedProject.workspaceId,
      forkedProject.name,
      forkedProject.manifestDraft,
      project1!!.componentsFileContent,
      forkedProject.baseActorDefinitionVersionId,
      forkedProject.contributionPullRequestUrl,
      forkedProject.contributionActorDefinitionId,
    )

    val project = connectorBuilderService!!.getConnectorBuilderProject(forkedProject.builderProjectId, false)
    org.junit.jupiter.api.Assertions
      .assertEquals(forkedADVId, project.baseActorDefinitionVersionId)
  }

  @Test
  fun testAddContributionInfo() {
    createBaseObjects()
    val contributionActorDefinitionId = UUID.randomUUID()
    val contributionPullRequestUrl = "https://github.com/airbytehq/airbyte/pull/1234"

    project1!!.contributionPullRequestUrl = contributionPullRequestUrl
    project1!!.contributionActorDefinitionId = contributionActorDefinitionId

    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.builderProjectId,
      project1!!.workspaceId,
      project1!!.name,
      project1!!.manifestDraft,
      project1!!.componentsFileContent,
      project1!!.baseActorDefinitionVersionId,
      project1!!.contributionPullRequestUrl,
      project1!!.contributionActorDefinitionId,
    )

    val updatedProject = connectorBuilderService!!.getConnectorBuilderProject(project1!!.builderProjectId, true)
    org.junit.jupiter.api.Assertions
      .assertEquals(contributionPullRequestUrl, updatedProject.contributionPullRequestUrl)
    org.junit.jupiter.api.Assertions
      .assertEquals(contributionActorDefinitionId, updatedProject.contributionActorDefinitionId)
  }

  private fun anyDeclarativeManifest(): DeclarativeManifest? {
    try {
      return DeclarativeManifest()
        .withActorDefinitionId(UUID.randomUUID())
        .withVersion(589345L)
        .withDescription("description for anyDeclarativeManifest")
        .withManifest(ObjectMapper().readTree("{}"))
        .withSpec(ObjectMapper().readTree("{}"))
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  private fun createBaseObjects() {
    mainWorkspace = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()

    project1 = createConnectorBuilderProject(mainWorkspace, "Z project", false)
    project2 = createConnectorBuilderProject(mainWorkspace, "A project", false)

    // deleted project, should not show up in listing
    createConnectorBuilderProject(mainWorkspace, "Deleted project", true)

    // unreachable project, should not show up in listing
    createConnectorBuilderProject(workspaceId2, "Other workspace project", false)
  }

  private fun createConnectorBuilderProject(
    workspace: UUID?,
    name: String?,
    deleted: Boolean,
  ): ConnectorBuilderProject {
    val projectId = UUID.randomUUID()
    val project =
      ConnectorBuilderProject()
        .withBuilderProjectId(projectId)
        .withName(name)
        .withTombstone(deleted)
        .withManifestDraft(ObjectMapper().readTree("{\"the_id\": \"$projectId\"}"))
        .withHasDraft(true)
        .withWorkspaceId(workspace)
    connectorBuilderService!!.writeBuilderProjectDraft(
      project.builderProjectId,
      project.workspaceId,
      project.name,
      project.manifestDraft,
      project.componentsFileContent,
      project.baseActorDefinitionVersionId,
      project.contributionPullRequestUrl,
      project.contributionActorDefinitionId,
    )
    if (deleted) {
      connectorBuilderService!!.deleteBuilderProject(project.builderProjectId)
    }
    return project
  }

  private fun linkSourceDefinition(projectId: UUID): StandardSourceDefinition {
    val id = UUID.randomUUID()

    val sourceDefinition =
      StandardSourceDefinition()
        .withName("source-def-$id")
        .withSourceDefinitionId(id)
        .withTombstone(false)

    val actorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withDockerRepository("repo-$id")
        .withDockerImageTag("0.0.1")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withSpec(ConnectorSpecification().withProtocolVersion("0.1.0"))

    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
    connectorBuilderService!!.insertActiveDeclarativeManifest(
      DeclarativeManifest()
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersion(MANIFEST_VERSION)
        .withDescription("")
        .withManifest(Jsons.emptyObject())
        .withSpec(Jsons.emptyObject()),
    )
    connectorBuilderService!!.assignActorDefinitionToConnectorBuilderProject(projectId, sourceDefinition.sourceDefinitionId)

    return sourceDefinition
  }

  companion object {
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.randomUUID()
    private const val MANIFEST_VERSION = 123L
    private val A_BUILDER_PROJECT_ID: UUID = UUID.randomUUID()
    private val A_SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val A_WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val A_PROJECT_NAME = "a project name"
    private const val ANOTHER_PROJECT_NAME = "another project name"
    private val ANY_UUID: UUID = UUID.randomUUID()
    private const val A_DESCRIPTION = "a description"
    private const val ACTIVE_MANIFEST_VERSION = 305L
    private val A_MANIFEST: JsonNode?
    private const val A_COMPONENTS_FILE_CONTENT = "a = 1"
    private val ANOTHER_MANIFEST: JsonNode?
    private const val UPDATED_AT = "updatedAt"

    init {
      try {
        A_MANIFEST = ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}")
        ANOTHER_MANIFEST = ObjectMapper().readTree("{\"another_manifest\": \"another_value\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }
  }
}
