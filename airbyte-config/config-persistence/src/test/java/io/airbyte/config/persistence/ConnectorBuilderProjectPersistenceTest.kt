/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ActorDefinitionBreakingChange
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
import org.junit.function.ThrowingRunnable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.io.IOException
import java.util.Arrays
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
  @Throws(Exception::class)
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
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testRead() {
    createBaseObjects()
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.getUpdatedAt())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testReadWithoutManifest() {
    createBaseObjects()
    project1!!.setManifestDraft(null)
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), false)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.getUpdatedAt())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testReadWithLinkedDefinition() {
    createBaseObjects()

    val sourceDefinition = linkSourceDefinition(project1!!.getBuilderProjectId())

    // project 1 should be associated with the newly created source definition
    project1!!.setActorDefinitionId(sourceDefinition.getSourceDefinitionId())
    project1!!.setActiveDeclarativeManifestVersion(MANIFEST_VERSION)

    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.getUpdatedAt())
  }

  @Test
  fun testReadNotExists() {
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.getConnectorBuilderProject(
          UUID.randomUUID(),
          false,
        )
      },
    )
  }

  @Test
  @Throws(IOException::class)
  fun testList() {
    createBaseObjects()

    // set draft to null because it won't be returned as part of listing call
    project1!!.setManifestDraft(null)
    project2!!.setManifestDraft(null)

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList<ConnectorBuilderProject?>( // project2 comes first due to alphabetical ordering
          Arrays.asList<ConnectorBuilderProject?>(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(0)!!.getUpdatedAt())
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(1)!!.getUpdatedAt())
  }

  @Test
  @Throws(IOException::class)
  fun testListWithLinkedDefinition() {
    createBaseObjects()

    val sourceDefinition = linkSourceDefinition(project1!!.getBuilderProjectId())

    // set draft to null because it won't be returned as part of listing call
    project1!!.setManifestDraft(null)
    project2!!.setManifestDraft(null)

    // project 1 should be associated with the newly created source definition
    project1!!.setActiveDeclarativeManifestVersion(MANIFEST_VERSION)
    project1!!.setActorDefinitionId(sourceDefinition.getSourceDefinitionId())

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList<ConnectorBuilderProject?>( // project2 comes first due to alphabetical ordering
          Arrays.asList<ConnectorBuilderProject?>(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(0)!!.getUpdatedAt())
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(1)!!.getUpdatedAt())
  }

  @Test
  @Throws(IOException::class)
  fun testListWithNoManifest() {
    createBaseObjects()

    // actually set draft to null for first project
    project1!!.setManifestDraft(null)
    project1!!.setHasDraft(false)
    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.getBuilderProjectId(),
      project1!!.getWorkspaceId(),
      project1!!.getName(),
      null,
      null,
      project1!!.getBaseActorDefinitionVersionId(),
      project1!!.getContributionPullRequestUrl(),
      project1!!.getContributionActorDefinitionId(),
    )

    // set draft to null because it won't be returned as part of listing call
    project2!!.setManifestDraft(null)
    // has draft is still truthy because there is a draft in the database
    project2!!.setHasDraft(true)

    val projects = connectorBuilderService!!.getConnectorBuilderProjectsByWorkspace(mainWorkspace!!).toList()

    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(
        ArrayList<ConnectorBuilderProject?>( // project2 comes first due to alphabetical ordering
          Arrays.asList<ConnectorBuilderProject?>(project2, project1),
        ),
      ).usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(projects)
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(0)!!.getUpdatedAt())
    org.junit.jupiter.api.Assertions
      .assertNotNull(projects.get(1)!!.getUpdatedAt())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdate() {
    createBaseObjects()
    project1!!.setName("Updated name")
    project1!!.setManifestDraft(ObjectMapper().readTree("{}"))
    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.getBuilderProjectId(),
      project1!!.getWorkspaceId(),
      project1!!.getName(),
      project1!!.getManifestDraft(),
      project1!!.getComponentsFileContent(),
      project1!!.getBaseActorDefinitionVersionId(),
      project1!!.getContributionPullRequestUrl(),
      project1!!.getContributionActorDefinitionId(),
    )
    val project = connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true)
    // `updatedAt` is populated by DB at insertion so we exclude from the equality check while
    // separately asserting it isn't null
    Assertions
      .assertThat<ConnectorBuilderProject?>(project1)
      .usingRecursiveComparison()
      .ignoringFields(UPDATED_AT)
      .isEqualTo(project)
    org.junit.jupiter.api.Assertions
      .assertNotNull(project.getUpdatedAt())
  }

  @Test
  @Throws(Exception::class)
  fun whenUpdateBuilderProjectAndActorDefinitionThenUpdateConnectorBuilderAndActorDefinition() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0)!!.withWorkspaceId(A_WORKSPACE_ID))
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
      .assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.getName())
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.getManifestDraft())
    org.junit.jupiter.api.Assertions.assertEquals(
      ANOTHER_PROJECT_NAME,
      sourceService!!.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName(),
    )
  }

  @Test
  @Throws(Exception::class)
  fun givenSourceIsPublicWhenUpdateBuilderProjectAndActorDefinitionThenActorDefinitionNameIsNotUpdated() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0)!!.withWorkspaceId(A_WORKSPACE_ID))
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
      .assertEquals(A_PROJECT_NAME, sourceService!!.getStandardSourceDefinition(A_SOURCE_DEFINITION_ID).getName())
  }

  @Test
  @Throws(Exception::class)
  fun testUpdateWithComponentsFile() {
    connectorBuilderService!!.writeBuilderProjectDraft(A_BUILDER_PROJECT_ID, A_WORKSPACE_ID, A_PROJECT_NAME, A_MANIFEST, null, null, null, null)
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0)!!.withWorkspaceId(A_WORKSPACE_ID))
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
      .assertEquals(ANOTHER_PROJECT_NAME, updatedConnectorBuilder.getName())
    org.junit.jupiter.api.Assertions
      .assertEquals(ANOTHER_MANIFEST, updatedConnectorBuilder.getManifestDraft())
    org.junit.jupiter.api.Assertions
      .assertEquals(A_COMPONENTS_FILE_CONTENT, updatedConnectorBuilder.getComponentsFileContent())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testDelete() {
    createBaseObjects()

    val deleted = connectorBuilderService!!.deleteBuilderProject(project1!!.getBuilderProjectId())
    org.junit.jupiter.api.Assertions
      .assertTrue(deleted)
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.getConnectorBuilderProject(
          project1!!.getBuilderProjectId(),
          false,
        )
      },
    )
    org.junit.jupiter.api.Assertions
      .assertNotNull(connectorBuilderService!!.getConnectorBuilderProject(project2!!.getBuilderProjectId(), false))
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testAssignActorDefinitionToConnectorBuilderProject() {
    val connectorBuilderProject = createConnectorBuilderProject(UUID.randomUUID(), "any", false)
    val aNewActorDefinitionId = UUID.randomUUID()

    connectorBuilderService!!.assignActorDefinitionToConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), aNewActorDefinitionId)

    org.junit.jupiter.api.Assertions.assertEquals(
      aNewActorDefinitionId,
      connectorBuilderService!!.getConnectorBuilderProject(connectorBuilderProject.getBuilderProjectId(), false).getActorDefinitionId(),
    )
  }

  @Test
  fun givenProjectDoesNotExistWhenGetVersionedConnectorBuilderProjectThenThrowException() {
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.getVersionedConnectorBuilderProject(
          UUID.randomUUID(),
          1L,
        )
      },
    )
  }

  @Test
  @Throws(IOException::class)
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
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.getVersionedConnectorBuilderProject(
          A_BUILDER_PROJECT_ID,
          1L,
        )
      },
    )
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
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
      .assertEquals(A_PROJECT_NAME, versionedConnectorBuilderProject.getName())
    org.junit.jupiter.api.Assertions
      .assertEquals(A_BUILDER_PROJECT_ID, versionedConnectorBuilderProject.getBuilderProjectId())
    org.junit.jupiter.api.Assertions
      .assertEquals(A_SOURCE_DEFINITION_ID, versionedConnectorBuilderProject.getSourceDefinitionId())
    org.junit.jupiter.api.Assertions
      .assertEquals(ACTIVE_MANIFEST_VERSION, versionedConnectorBuilderProject.getActiveDeclarativeManifestVersion())
    org.junit.jupiter.api.Assertions
      .assertEquals(MANIFEST_VERSION, versionedConnectorBuilderProject.getManifestVersion())
    org.junit.jupiter.api.Assertions
      .assertEquals(A_DESCRIPTION, versionedConnectorBuilderProject.getManifestDescription())
    org.junit.jupiter.api.Assertions
      .assertEquals(A_MANIFEST, versionedConnectorBuilderProject.getManifest())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testDeleteBuilderProjectDraft() {
    createBaseObjects()
    org.junit.jupiter.api.Assertions.assertNotNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true).getManifestDraft(),
    )
    connectorBuilderService!!.deleteBuilderProjectDraft(project1!!.getBuilderProjectId())
    org.junit.jupiter.api.Assertions.assertNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true).getManifestDraft(),
    )
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testDeleteManifestDraftForActorDefinitionId() {
    createBaseObjects()
    val sourceDefinition = linkSourceDefinition(project1!!.getBuilderProjectId())
    org.junit.jupiter.api.Assertions.assertNotNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true).getManifestDraft(),
    )
    connectorBuilderService!!.deleteManifestDraftForActorDefinition(sourceDefinition.getSourceDefinitionId(), project1!!.getWorkspaceId())
    org.junit.jupiter.api.Assertions.assertNull(
      connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true).getManifestDraft(),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testGetConnectorBuilderProjectIdByActorDefinitionId() {
    createBaseObjects()
    val sourceDefinition = linkSourceDefinition(project1!!.getBuilderProjectId())
    org.junit.jupiter.api.Assertions.assertEquals(
      Optional.of<UUID?>(project1!!.getBuilderProjectId()),
      connectorBuilderService!!.getConnectorBuilderProjectIdForActorDefinitionId(sourceDefinition.getSourceDefinitionId()),
    )
  }

  @Test
  @Throws(IOException::class)
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
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testCreateForkedProject() {
    createBaseObjects()

    // Create ADV and StandardSourceDefinition for DB constraints
    val forkedADVId = UUID.randomUUID()
    val forkedSourceDefId = UUID.randomUUID()
    val forkedADV = MockData.actorDefinitionVersion()!!.withVersionId(forkedADVId).withActorDefinitionId(forkedSourceDefId)
    sourceService!!.writeConnectorMetadata(
      MockData.standardSourceDefinitions().get(0)!!.withSourceDefinitionId(forkedSourceDefId),
      forkedADV,
      mutableListOf<ActorDefinitionBreakingChange>(),
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
      forkedProject.getBuilderProjectId(),
      forkedProject.getWorkspaceId(),
      forkedProject.getName(),
      forkedProject.getManifestDraft(),
      project1!!.getComponentsFileContent(),
      forkedProject.getBaseActorDefinitionVersionId(),
      forkedProject.getContributionPullRequestUrl(),
      forkedProject.getContributionActorDefinitionId(),
    )

    val project = connectorBuilderService!!.getConnectorBuilderProject(forkedProject.getBuilderProjectId(), false)
    org.junit.jupiter.api.Assertions
      .assertEquals(forkedADVId, project.getBaseActorDefinitionVersionId())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testAddContributionInfo() {
    createBaseObjects()
    val contributionActorDefinitionId = UUID.randomUUID()
    val contributionPullRequestUrl = "https://github.com/airbytehq/airbyte/pull/1234"

    project1!!.setContributionPullRequestUrl(contributionPullRequestUrl)
    project1!!.setContributionActorDefinitionId(contributionActorDefinitionId)

    connectorBuilderService!!.writeBuilderProjectDraft(
      project1!!.getBuilderProjectId(),
      project1!!.getWorkspaceId(),
      project1!!.getName(),
      project1!!.getManifestDraft(),
      project1!!.getComponentsFileContent(),
      project1!!.getBaseActorDefinitionVersionId(),
      project1!!.getContributionPullRequestUrl(),
      project1!!.getContributionActorDefinitionId(),
    )

    val updatedProject = connectorBuilderService!!.getConnectorBuilderProject(project1!!.getBuilderProjectId(), true)
    org.junit.jupiter.api.Assertions
      .assertEquals(contributionPullRequestUrl, updatedProject.getContributionPullRequestUrl())
    org.junit.jupiter.api.Assertions
      .assertEquals(contributionActorDefinitionId, updatedProject.getContributionActorDefinitionId())
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

  @Throws(IOException::class)
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

  @Throws(IOException::class)
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
        .withManifestDraft(ObjectMapper().readTree("{\"the_id\": \"" + projectId + "\"}"))
        .withHasDraft(true)
        .withWorkspaceId(workspace)
    connectorBuilderService!!.writeBuilderProjectDraft(
      project.getBuilderProjectId(),
      project.getWorkspaceId(),
      project.getName(),
      project.getManifestDraft(),
      project.getComponentsFileContent(),
      project.getBaseActorDefinitionVersionId(),
      project.getContributionPullRequestUrl(),
      project.getContributionActorDefinitionId(),
    )
    if (deleted) {
      connectorBuilderService!!.deleteBuilderProject(project.getBuilderProjectId())
    }
    return project
  }

  @Throws(IOException::class)
  private fun linkSourceDefinition(projectId: UUID): StandardSourceDefinition {
    val id = UUID.randomUUID()

    val sourceDefinition =
      StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)

    val actorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withDockerRepository("repo-" + id)
        .withDockerImageTag("0.0.1")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withSpec(ConnectorSpecification().withProtocolVersion("0.1.0"))

    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())
    connectorBuilderService!!.insertActiveDeclarativeManifest(
      DeclarativeManifest()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersion(MANIFEST_VERSION)
        .withDescription("")
        .withManifest(Jsons.emptyObject())
        .withSpec(Jsons.emptyObject()),
    )
    connectorBuilderService!!.assignActorDefinitionToConnectorBuilderProject(projectId, sourceDefinition.getSourceDefinitionId())

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
