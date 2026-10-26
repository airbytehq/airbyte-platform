/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.server.generated.models.BaseActorDefinitionVersionInfo
import io.airbyte.api.server.generated.models.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.server.generated.models.BuilderProjectOauthConsentRequest
import io.airbyte.api.server.generated.models.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.ConnectorBuilderHttpRequest
import io.airbyte.api.server.generated.models.ConnectorBuilderHttpResponse
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectDetails
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectFullResolveRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadSlicesInner
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderPublishRequestBody
import io.airbyte.api.server.generated.models.DeclarativeManifestRequestBody
import io.airbyte.api.server.generated.models.DeclarativeSourceManifest
import io.airbyte.api.server.generated.models.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.constants.AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.server.builder.manifest.processor.ManifestServerManifestProcessor
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistenceFactory
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

internal class ConnectorBuilderProjectsHandlerTest {
  private lateinit var declarativeManifestImageVersionService: DeclarativeManifestImageVersionService
  private lateinit var connectorBuilderService: ConnectorBuilderService
  private lateinit var builderProjectUpdater: BuilderProjectUpdater
  private lateinit var connectorBuilderProjectsHandler: ConnectorBuilderProjectsHandler
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var manifestInjector: DeclarativeSourceManifestInjector
  private lateinit var workspaceService: WorkspaceService
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretPersistenceConfigService: SecretPersistenceConfigService
  private lateinit var sourceService: SourceService
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var manifestServerProcessor: ManifestServerManifestProcessor
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var adaptedConnectorSpecification: ConnectorSpecification
  private lateinit var oauthImplementationFactory: OAuthImplementationFactory
  private lateinit var runtimeSecretPersistenceFactory: RuntimeSecretPersistenceFactory
  private lateinit var workspaceId: UUID
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
  private val draftManifest: JsonNode = addSpec(deserialize("{\"test\":123,\"empty\":{\"array_in_object\":[]}}"))
  private val testingValues =
    deserialize(
      """
      {
        "username": "bob",
        "password": "hunter2"
      }
      """.trimIndent(),
    )
  private val testingValuesWithSecretCoordinates =
    deserialize(
      """
      {
        "username": "bob",
        "password": {
          "_secret": "airbyte_workspace_123_secret_456_v1"
        }
      }
      """.trimIndent(),
    )
  private val testingValuesWithObfuscatedSecrets =
    deserialize(
      """
      {
        "username": "bob",
        "password": "**********"
      }
      """.trimIndent(),
    )

  @BeforeEach
  fun setUp() {
    declarativeManifestImageVersionService =
      mockk<DeclarativeManifestImageVersionService>(relaxUnitFun = true)
    connectorBuilderService = mockk<ConnectorBuilderService>(relaxUnitFun = true)
    builderProjectUpdater = mockk<BuilderProjectUpdater>(relaxUnitFun = true)
    uuidSupplier = mockk<Supplier<UUID>>()
    manifestInjector = mockk<DeclarativeSourceManifestInjector>(relaxUnitFun = true)
    workspaceService = mockk<WorkspaceService>(relaxUnitFun = true)
    featureFlagClient = mockk<TestClient>(relaxUnitFun = true)
    secretsRepositoryReader = mockk<SecretsRepositoryReader>(relaxUnitFun = true)
    secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxUnitFun = true)
    secretPersistenceConfigService = mockk<SecretPersistenceConfigService>(relaxUnitFun = true)
    sourceService = mockk<SourceService>(relaxUnitFun = true)
    secretsProcessor = mockk<JsonSecretsProcessor>(relaxUnitFun = true)
    manifestServerProcessor = mockk<ManifestServerManifestProcessor>(relaxed = true)
    actorDefinitionService = mockk<ActorDefinitionService>(relaxUnitFun = true)
    remoteDefinitionsProvider = mockk<RemoteDefinitionsProvider>(relaxUnitFun = true)
    adaptedConnectorSpecification = mockk<ConnectorSpecification>(relaxUnitFun = true)
    oauthImplementationFactory = mockk<OAuthImplementationFactory>(relaxUnitFun = true)
    runtimeSecretPersistenceFactory = mockk<RuntimeSecretPersistenceFactory>(relaxUnitFun = true)
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(any()) } returns adaptedConnectorSpecification
    every { adaptedConnectorSpecification.documentationUrl } returns URI.create("")
    every { workspaceService.getOrganizationIdFromWorkspaceId(any()) } returns Optional.empty()
    every { remoteDefinitionsProvider.getConnectorCustomComponents(any(), any()) } returns Optional.empty()
    every { connectorBuilderService.deleteBuilderProject(any()) } returns false
    every { actorDefinitionService.getActorDefinitionVersions(any()) } returns emptyList()
    every { manifestInjector.getManifestConnectorInjections(any(), any(), any()) } returns emptyList()
    every { sourceService.listStandardSourceDefinitions(any()) } returns emptyList()
    workspaceId = UUID.randomUUID()

    connectorBuilderProjectsHandler =
      ConnectorBuilderProjectsHandler(
        declarativeManifestImageVersionService,
        connectorBuilderService,
        builderProjectUpdater,
        uuidSupplier,
        manifestInjector,
        workspaceService,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        sourceService,
        secretsProcessor,
        manifestServerProcessor,
        actorDefinitionService,
        remoteDefinitionsProvider,
        oauthImplementationFactory,
        runtimeSecretPersistenceFactory,
      )

    every { manifestInjector.getCdkVersion(any()) } returns A_CDK_VERSION
    every {
      declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
        any(),
      )
    } returns A_DECLARATIVE_MANIFEST_IMAGE_VERSION

    // manifestServerProcessor is mocked and used directly
  }

  private fun generateBuilderProject(): ConnectorBuilderProject {
    val projectId = UUID.randomUUID()
    return ConnectorBuilderProject()
      .withBuilderProjectId(projectId)
      .withWorkspaceId(workspaceId)
      .withName("Test project")
      .withHasDraft(true)
      .withManifestDraft(draftManifest)
  }

  @Test
  @DisplayName("createConnectorBuilderProject should create a new project and return the id")
  fun testCreateConnectorBuilderProject() {
    val project = generateBuilderProject()

    every { uuidSupplier.get() } returns project.builderProjectId

    val create =
      ConnectorBuilderProjectWithWorkspaceId(
        builderProject = ConnectorBuilderProjectDetails(name = project.name, draftManifest = project.manifestDraft),
        workspaceId = workspaceId,
      )

    val response = connectorBuilderProjectsHandler.createConnectorBuilderProject(create)
    Assertions.assertEquals(project.builderProjectId, response.builderProjectId)
    Assertions.assertEquals(project.workspaceId, response.workspaceId)

    verify(exactly = 1) {
      connectorBuilderService.writeBuilderProjectDraft(
        project.builderProjectId,
        project.workspaceId,
        project.name,
        project.manifestDraft,
        null,
        project.baseActorDefinitionVersionId,
        project.contributionPullRequestUrl,
        project.contributionActorDefinitionId,
      )
    }
  }

  @Test
  @DisplayName("publishConnectorBuilderProject throws a helpful error if no associated CDK version is found")
  fun testCreateConnectorBuilderProjectNoCdkVersion() {
    val project = generateBuilderProject()
    every {
      connectorBuilderService.getConnectorBuilderProject(
        any(),
        any(),
      )
    } returns project

    every { uuidSupplier.get() } returns project.builderProjectId
    every {
      declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
        any(),
      )
    } throws IllegalStateException("No declarative manifest image version found in database for major version 0")

    val publish =
      ConnectorBuilderPublishRequestBody(
        name = "",
        builderProjectId = project.builderProjectId,
        workspaceId = workspaceId,
        initialDeclarativeManifest = anyInitialManifest(),
      )

    Assertions.assertEquals(
      "No declarative manifest image version found in database for major version 0",
      Assertions
        .assertThrows(
          IllegalStateException::class.java,
        ) { connectorBuilderProjectsHandler.publishConnectorBuilderProject(publish) }
        .message,
    )
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project")
  fun testUpdateConnectorBuilderProject() {
    val project = generateBuilderProject()

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId(
        builderProject = ConnectorBuilderProjectDetails(name = project.name, draftManifest = project.getManifestDraft()),
        workspaceId = workspaceId,
        builderProjectId = project.builderProjectId,
      )

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(update)

    verify(exactly = 1) {
      builderProjectUpdater.persistBuilderProjectUpdate(
        update,
      )
    }
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should validate whether the workspace does not match")
  fun testUpdateConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    val wrongWorkspace = UUID.randomUUID()

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId(
        builderProject = ConnectorBuilderProjectDetails(name = project.name, draftManifest = project.manifestDraft),
        workspaceId = workspaceId,
        builderProjectId = project.builderProjectId,
      )

    project.setWorkspaceId(wrongWorkspace)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        false,
      )
    } returns project

    Assertions.assertThrows(ConfigNotFoundException::class.java) {
      connectorBuilderProjectsHandler.updateConnectorBuilderProject(update)
    }

    verify(exactly = 0) {
      connectorBuilderService.writeBuilderProjectDraft(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  fun testDeleteConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    val wrongWorkspace = UUID.randomUUID()

    project.workspaceId = wrongWorkspace
    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
      {
        connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
          ConnectorBuilderProjectIdWithWorkspaceId(
            builderProjectId = project.builderProjectId,
            workspaceId = workspaceId,
          ),
        )
      },
    )

    verify(exactly = 0) { connectorBuilderService.deleteBuilderProject(any()) }
  }

  @Test
  @DisplayName("publishBuilderProject should validate whether the workspace does not match")
  fun testPublishConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project

    val wrongWorkspaceId = UUID.randomUUID()
    val publishReq: ConnectorBuilderPublishRequestBody =
      anyConnectorBuilderProjectRequest(builderProjectId = project.builderProjectId, workspaceId = wrongWorkspaceId)

    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
      { connectorBuilderProjectsHandler.publishConnectorBuilderProject(publishReq) },
    )
    verify(exactly = 0) {
      connectorBuilderService.insertActiveDeclarativeManifest(
        any(),
      )
    }
    verify(exactly = 0) {
      connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(
        any(),
        any(),
      )
    }
    verify(exactly = 0) { connectorBuilderService.deleteBuilderProjectDraft(any()) }
  }

  @Test
  fun testProjectTestingValuesEndpointsValidateWorkspace() {
    val project = generateBuilderProject()
    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project
    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, true) } returns project
    val wrongWorkspaceId = UUID.randomUUID()

    listOf(
      {
        connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(
          ConnectorBuilderProjectStreamReadRequestBody(
            builderProjectId = project.builderProjectId,
            workspaceId = wrongWorkspaceId,
            manifest = A_MANIFEST,
            streamName = "stream1",
          ),
        )
      },
      {
        connectorBuilderProjectsHandler.fullResolveManifestBuilderProject(
          ConnectorBuilderProjectFullResolveRequestBody(
            manifest = A_MANIFEST,
            builderProjectId = project.builderProjectId,
            workspaceId = wrongWorkspaceId,
          ),
        )
      },
      {
        connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(
          BuilderProjectOauthConsentRequest(
            builderProjectId = project.builderProjectId,
            workspaceId = wrongWorkspaceId,
            redirectUrl = "https://redirect.url",
          ),
        )
      },
      {
        connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(
          CompleteConnectorBuilderProjectOauthRequest(
            builderProjectId = project.builderProjectId,
            workspaceId = wrongWorkspaceId,
          ),
        )
      },
    ).forEach { call -> Assertions.assertThrows(ConfigNotFoundException::class.java) { call() } }

    // the testing values of a project in another workspace are never hydrated
    verify { secretsRepositoryReader wasNot Called }
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should delete an existing project")
  fun testDeleteConnectorBuilderProject() {
    val project = generateBuilderProject()

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project

    connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
      ConnectorBuilderProjectIdWithWorkspaceId(
        builderProjectId = project.builderProjectId,
        workspaceId = workspaceId,
      ),
    )

    verify(exactly = 1) {
      connectorBuilderService.deleteBuilderProject(
        project.builderProjectId,
      )
    }
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list all projects without drafts")
  fun testListConnectorBuilderProject() {
    val project1 = generateBuilderProject()
    val project2 = generateBuilderProject()
    project2.setHasDraft(false)
    project1.setActiveDeclarativeManifestVersion(A_VERSION)
    project1.setActorDefinitionId(UUID.randomUUID())

    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } returns listOf(project1, project2).stream()

    val response =
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(WorkspaceIdRequestBody(workspaceId = workspaceId))

    Assertions.assertEquals(project1.builderProjectId, response.projects[0].builderProjectId)
    Assertions.assertEquals(project2.builderProjectId, response.projects[1].builderProjectId)

    Assertions.assertTrue(response.projects[0].hasDraft)
    Assertions.assertFalse(response.projects[1].hasDraft)

    Assertions.assertEquals(project1.activeDeclarativeManifestVersion, response.projects[0].activeDeclarativeManifestVersion)
    Assertions.assertEquals(project1.actorDefinitionId, response.projects[0].sourceDefinitionId)
    Assertions.assertNull(project2.activeDeclarativeManifestVersion)
    Assertions.assertNull(project2.actorDefinitionId)

    verify(exactly = 1) {
      connectorBuilderService.getConnectorBuilderProjectsByWorkspace(
        workspaceId,
      )
    }
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list both forked and non-forked projects")
  fun testListForkedAndNonForkedProjects() {
    val unforkedProject = generateBuilderProject()
    val forkedProject = generateBuilderProject().withActorDefinitionId(UUID.randomUUID())
    forkedProject.baseActorDefinitionVersionId = FORKED_ADV.versionId

    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } returns listOf(unforkedProject, forkedProject).stream()
    every { actorDefinitionService.getActorDefinitionVersions(listOf(FORKED_ADV.versionId)) } returns
      listOf(
        FORKED_ADV,
      )
    every { sourceService.listStandardSourceDefinitions(false) } returns
      listOf(
        StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()),
        FORKED_SOURCE,
      )

    val response =
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(WorkspaceIdRequestBody(workspaceId = workspaceId))

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo(
        name = FORKED_SOURCE.name,
        dockerRepository = FORKED_ADV.dockerRepository,
        dockerImageTag = FORKED_ADV.dockerImageTag,
        actorDefinitionId = FORKED_SOURCE.getSourceDefinitionId(),
        icon = FORKED_SOURCE.getIconUrl(),
        documentationUrl = FORKED_ADV.documentationUrl,
      )

    Assertions.assertEquals(2, response.projects.size)
    Assertions.assertEquals(unforkedProject.builderProjectId, response.projects[0].builderProjectId)
    Assertions.assertEquals(forkedProject.builderProjectId, response.projects[1].builderProjectId)
    Assertions.assertNull(response.projects[0].baseActorDefinitionVersionInfo)
    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.projects[1].baseActorDefinitionVersionInfo)
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list a forked project whose base source definition has no icon")
  fun testListForkedProjectWithBaseSourceDefinitionWithoutIcon() {
    val forkedProject = generateBuilderProject().withActorDefinitionId(UUID.randomUUID())
    forkedProject.baseActorDefinitionVersionId = FORKED_ADV.versionId
    val sourceWithoutIcon =
      StandardSourceDefinition()
        .withSourceDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .withName(FORKED_SOURCE.name)

    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } returns listOf(forkedProject).stream()
    every { actorDefinitionService.getActorDefinitionVersions(listOf(FORKED_ADV.versionId)) } returns listOf(FORKED_ADV)
    every { sourceService.listStandardSourceDefinitions(false) } returns listOf(sourceWithoutIcon)

    val response =
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(WorkspaceIdRequestBody(workspaceId = workspaceId))

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo(
        name = FORKED_SOURCE.name,
        dockerRepository = FORKED_ADV.dockerRepository,
        dockerImageTag = FORKED_ADV.dockerImageTag,
        actorDefinitionId = FORKED_SOURCE.getSourceDefinitionId(),
        icon = null,
        documentationUrl = FORKED_ADV.documentationUrl,
      )

    Assertions.assertEquals(1, response.projects.size)
    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.projects[0].baseActorDefinitionVersionInfo)
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  fun testGetConnectorBuilderProject() {
    val project = generateBuilderProject()
    project.setActorDefinitionId(UUID.randomUUID())
    project.setActiveDeclarativeManifestVersion(A_VERSION)
    project.setTestingValues(testingValuesWithSecretCoordinates)

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project
    every { secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)) } returns
      testingValuesWithObfuscatedSecrets

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertEquals(project.actorDefinitionId, response.builderProject.sourceDefinitionId)
    Assertions.assertEquals(project.getActiveDeclarativeManifestVersion(), response.builderProject.activeDeclarativeManifestVersion)
    Assertions.assertTrue(response.declarativeManifest!!.isDraft!!)
    Assertions.assertEquals(
      serialize<JsonNode?>(draftManifest),
      ObjectMapper().writeValueAsString(response.declarativeManifest!!.manifest),
    )
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.testingValues)
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and null testing values if it doesn't have any")
  fun testGetConnectorBuilderProjectNullTestingValues() {
    val project = generateBuilderProject()
    project.setActorDefinitionId(UUID.randomUUID())
    project.setActiveDeclarativeManifestVersion(A_VERSION)
    project.setTestingValues(null)

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertEquals(project.actorDefinitionId, response.builderProject.sourceDefinitionId)
    Assertions.assertEquals(project.getActiveDeclarativeManifestVersion(), response.builderProject.activeDeclarativeManifestVersion)
    Assertions.assertTrue(response.declarativeManifest!!.isDraft!!)
    Assertions.assertEquals(
      serialize<JsonNode?>(draftManifest),
      ObjectMapper().writeValueAsString(response.declarativeManifest!!.manifest),
    )
    Assertions.assertNull(response.testingValues)
    verify(exactly = 0) { secretsProcessor.prepareSecretsForOutput(any(), any()) }
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  fun testGetConnectorBuilderProjectWithoutDraft() {
    val project = generateBuilderProject()
    project.setManifestDraft(null)
    project.setHasDraft(false)
    project.setTestingValues(null)

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertFalse(response.builderProject.hasDraft)
    Assertions.assertNull(response.declarativeManifest)
    Assertions.assertNull(response.testingValues)
    verify(exactly = 0) { secretsProcessor.prepareSecretsForOutput(any(), any()) }
  }

  @Test
  fun testGetConnectorBuilderProjectWithBaseActorDefinitionVersion() {
    val project = generateBuilderProject()
    project.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId())

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project
    every { actorDefinitionService.getActorDefinitionVersion(FORKED_ADV.getVersionId()) } returns FORKED_ADV
    every { sourceService.getStandardSourceDefinition(FORKED_SOURCE.getSourceDefinitionId()) } returns FORKED_SOURCE

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo(
        name = FORKED_SOURCE.name,
        dockerRepository = FORKED_ADV.dockerRepository,
        dockerImageTag = FORKED_ADV.dockerImageTag,
        actorDefinitionId = FORKED_SOURCE.getSourceDefinitionId(),
        icon = FORKED_SOURCE.getIconUrl(),
        documentationUrl = FORKED_ADV.documentationUrl,
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.builderProject.baseActorDefinitionVersionInfo)
  }

  @Test
  fun testGetConnectorBuilderProjectWithBaseSourceDefinitionWithoutIcon() {
    val project = generateBuilderProject()
    project.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId())
    val sourceWithoutIcon =
      StandardSourceDefinition()
        .withSourceDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .withName(FORKED_SOURCE.name)

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project
    every { actorDefinitionService.getActorDefinitionVersion(FORKED_ADV.getVersionId()) } returns FORKED_ADV
    every { sourceService.getStandardSourceDefinition(FORKED_SOURCE.getSourceDefinitionId()) } returns sourceWithoutIcon

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo(
        name = FORKED_SOURCE.name,
        dockerRepository = FORKED_ADV.dockerRepository,
        dockerImageTag = FORKED_ADV.dockerImageTag,
        actorDefinitionId = FORKED_SOURCE.getSourceDefinitionId(),
        icon = null,
        documentationUrl = FORKED_ADV.documentationUrl,
      )

    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.builderProject.baseActorDefinitionVersionInfo)
  }

  @Test
  fun testGetConnectorBuilderProjectWithContributionInfo() {
    val project = generateBuilderProject()
    project.setContributionPullRequestUrl(A_PULL_REQUEST_URL)
    project.setContributionActorDefinitionId(A_CONTRIBUTION_ACTOR_DEFINITION_ID)

    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertEquals(A_PULL_REQUEST_URL, response.builderProject.contributionInfo!!.pullRequestUrl)
    Assertions.assertEquals(A_CONTRIBUTION_ACTOR_DEFINITION_ID, response.builderProject.contributionInfo!!.actorDefinitionId)
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  fun givenNoVersionButActiveManifestWhenGetConnectorBuilderProjectWithManifestThenReturnActiveVersion() {
    val project =
      generateBuilderProject()
        .withManifestDraft(null)
        .withHasDraft(false)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withTestingValues(testingValuesWithSecretCoordinates)
    val manifest = addSpec(A_MANIFEST)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        project.builderProjectId,
        any(),
      )
    } returns project
    every {
      connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
        A_SOURCE_DEFINITION_ID,
      )
    } returns
      DeclarativeManifest()
        .withManifest(manifest)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
    every { secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)) } returns
      testingValuesWithObfuscatedSecrets

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(project.builderProjectId, response.builderProject.builderProjectId)
    Assertions.assertFalse(response.builderProject.hasDraft)
    Assertions.assertEquals(A_VERSION, response.declarativeManifest!!.version)
    Assertions.assertEquals(manifest, response.declarativeManifest!!.manifest)
    Assertions.assertEquals(false, response.declarativeManifest!!.isDraft)
    Assertions.assertEquals(A_DESCRIPTION, response.declarativeManifest!!.description)
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.testingValues)
  }

  @Test
  fun givenVersionWhenGetConnectorBuilderProjectWithManifestThenReturnSpecificVersion() {
    val manifest = addSpec(A_MANIFEST)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        A_BUILDER_PROJECT_ID,
        false,
      )
    } returns ConnectorBuilderProject().withWorkspaceId(A_WORKSPACE_ID)
    every {
      connectorBuilderService.getVersionedConnectorBuilderProject(
        A_BUILDER_PROJECT_ID,
        A_VERSION,
      )
    } returns
      ConnectorBuilderProjectVersionedManifest()
        .withBuilderProjectId(A_BUILDER_PROJECT_ID)
        .withActiveDeclarativeManifestVersion(ACTIVE_MANIFEST_VERSION)
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withHasDraft(true)
        .withName(A_NAME)
        .withManifest(manifest)
        .withManifestVersion(A_VERSION)
        .withManifestDescription(A_DESCRIPTION)
        .withTestingValues(testingValuesWithSecretCoordinates)
    every { secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)) } returns
      testingValuesWithObfuscatedSecrets

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId(
          builderProjectId = A_BUILDER_PROJECT_ID,
          workspaceId = A_WORKSPACE_ID,
          version = A_VERSION,
        ),
      )

    Assertions.assertEquals(A_BUILDER_PROJECT_ID, response.builderProject.builderProjectId)
    Assertions.assertEquals(A_NAME, response.builderProject.name)
    Assertions.assertEquals(ACTIVE_MANIFEST_VERSION, response.builderProject.activeDeclarativeManifestVersion)
    Assertions.assertEquals(A_SOURCE_DEFINITION_ID, response.builderProject.sourceDefinitionId)
    Assertions.assertEquals(true, response.builderProject.hasDraft)
    Assertions.assertEquals(A_VERSION, response.declarativeManifest!!.version)
    Assertions.assertEquals(manifest, response.declarativeManifest!!.manifest)
    Assertions.assertEquals(false, response.declarativeManifest!!.isDraft)
    Assertions.assertEquals(A_DESCRIPTION, response.declarativeManifest!!.description)
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.testingValues)
  }

  @Test
  fun whenPublishConnectorBuilderProjectThenReturnActorDefinition() {
    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID
    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        any(),
        any(),
      )
    } returns project

    val req: ConnectorBuilderPublishRequestBody =
      anyConnectorBuilderProjectRequest(
        builderProjectId = A_BUILDER_PROJECT_ID,
        workspaceId = A_WORKSPACE_ID,
        initialManifest = anyInitialManifest().copy(spec = emptyObject(), manifest = emptyObject()),
      )
    val response = connectorBuilderProjectsHandler.publishConnectorBuilderProject(req)
    Assertions.assertEquals(A_SOURCE_DEFINITION_ID, response.sourceDefinitionId)
  }

  @Test
  fun whenPublishConnectorBuilderProjectThenCreateActorDefinition() {
    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        A_MANIFEST,
        null,
      )
    } returns
      listOf(
        A_CONFIG_INJECTION,
      )
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL)

    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        any(),
        any(),
      )
    } returns project

    val organizationId = UUID.randomUUID()
    every {
      workspaceService.getOrganizationIdFromWorkspaceId(
        any(),
      )
    } returns Optional.of(organizationId)

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest(
        builderProjectId = A_BUILDER_PROJECT_ID,
        workspaceId = workspaceId,
        name = A_SOURCE_NAME,
      ),
    )

    verify(exactly = 1) {
      manifestInjector.addInjectedDeclarativeManifest(
        A_SPEC,
      )
    }
    verify(exactly = 1) {
      sourceService.writeCustomConnectorMetadata(
        StandardSourceDefinition()
          .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
          .withName(A_SOURCE_NAME)
          .withSourceType(StandardSourceDefinition.SourceType.CUSTOM)
          .withTombstone(false)
          .withPublic(false)
          .withCustom(true),
        ActorDefinitionVersion()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
          .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.imageVersion)
          .withSpec(adaptedConnectorSpecification)
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withDocumentationUrl(A_DOCUMENTATION_URL)
          .withProtocolVersion("0.2.0")
          .withSupportsFileTransfer(false),
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }

    verify(exactly = 1) {
      connectorBuilderService.writeActorDefinitionConfigInjectionsForPath(
//                A_SOURCE_DEFINITION_ID,
        listOf(A_CONFIG_INJECTION),
      )
    }
  }

  @Test
  fun whenNoOrganizationFoundShouldUseWorkspaceScope() {
    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        A_MANIFEST,
        null,
      )
    } returns
      listOf(
        A_CONFIG_INJECTION,
      )
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL)

    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId)
    every { connectorBuilderService.getConnectorBuilderProject(any(), any()) } returns project

    every { workspaceService.getOrganizationIdFromWorkspaceId(any()) } returns Optional.empty<UUID>()

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest(
        builderProjectId = A_BUILDER_PROJECT_ID,
        workspaceId = workspaceId,
        name = A_SOURCE_NAME,
      ),
    )

    verify(exactly = 1) {
      manifestInjector.addInjectedDeclarativeManifest(
        A_SPEC,
      )
    }
    verify(exactly = 1) {
      sourceService.writeCustomConnectorMetadata(
        StandardSourceDefinition()
          .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
          .withName(A_SOURCE_NAME)
          .withSourceType(StandardSourceDefinition.SourceType.CUSTOM)
          .withTombstone(false)
          .withPublic(false)
          .withCustom(true),
        ActorDefinitionVersion()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
          .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.imageVersion)
          .withSpec(adaptedConnectorSpecification)
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withDocumentationUrl(A_DOCUMENTATION_URL)
          .withProtocolVersion("0.2.0")
          .withSupportsFileTransfer(false),
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }

    verify(exactly = 1) {
      connectorBuilderService.writeActorDefinitionConfigInjectionsForPath(
//                A_SOURCE_DEFINITION_ID,
        listOf(A_CONFIG_INJECTION),
      )
    }
  }

  @Test
  fun whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() {
    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID
    val project = generateBuilderProject().withWorkspaceId(A_WORKSPACE_ID)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        any(),
        any(),
      )
    } returns project

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest(builderProjectId = A_BUILDER_PROJECT_ID, workspaceId = A_WORKSPACE_ID),
    )

    verify(exactly = 1) {
      connectorBuilderService.insertActiveDeclarativeManifest(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      )
    }
    verify(exactly = 1) {
      connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(
        A_BUILDER_PROJECT_ID,
        A_SOURCE_DEFINITION_ID,
      )
    }
  }

  @Test
  fun whenPublishConnectorBuilderProjectThenDraftDeleted() {
    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID)
    every {
      connectorBuilderService.getConnectorBuilderProject(
        any(),
        any(),
      )
    } returns project
    every { uuidSupplier.get() } returns project.builderProjectId

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest(builderProjectId = A_BUILDER_PROJECT_ID, workspaceId = A_WORKSPACE_ID),
    )

    verify(exactly = 1) { connectorBuilderService.deleteBuilderProjectDraft(A_BUILDER_PROJECT_ID) }
  }

  @Test
  fun testUpdateTestingValuesOnProjectWithNoExistingValues() {
    val project = generateBuilderProject()
    val spec =
      deserialize(
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
        """.trimIndent(),
      )

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project
    every { secretsRepositoryWriter.createFromConfigLegacy(workspaceId, testingValues, spec, null) } returns testingValuesWithSecretCoordinates
    every { secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, spec) } returns testingValuesWithObfuscatedSecrets

    val response =
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        ConnectorBuilderProjectTestingValuesUpdate(
          workspaceId = workspaceId,
          builderProjectId = project.builderProjectId,
          testingValues = testingValues,
          spec = spec,
        ),
      )
    Assertions.assertEquals(response, testingValuesWithObfuscatedSecrets)
    verify(exactly = 1) { connectorBuilderService.updateBuilderProjectTestingValues(project.builderProjectId, testingValuesWithSecretCoordinates) }
  }

  @Test
  fun testUpdateTestingValuesOnProjectWithExistingValues() {
    val project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates)
    val newTestingValues =
      deserialize(
        """
        {
          "username": "bob",
          "password": "hunter3"
        }
        """.trimIndent(),
      )
    val newTestingValuesWithSecretCoordinates =
      deserialize(
        """
        {
          "username": "bob",
          "password": {
            "_secret": "airbyte_workspace_123_secret_456_v2"
          }
        }
        """.trimIndent(),
      )
    val spec = deserialize(specString)

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues
    every { secretsProcessor.copySecrets(testingValues, newTestingValues, spec) } returns newTestingValues
    every {
      secretsRepositoryWriter.updateFromConfigLegacy(
        workspaceId,
        testingValuesWithSecretCoordinates,
        newTestingValues,
        spec,
        null,
      )
    } returns newTestingValuesWithSecretCoordinates
    every { secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec) } returns testingValuesWithObfuscatedSecrets

    val response =
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        ConnectorBuilderProjectTestingValuesUpdate(
          builderProjectId = project.builderProjectId,
          workspaceId = workspaceId,
          testingValues = newTestingValues,
          spec = spec,
        ),
      )
    Assertions.assertEquals(response, testingValuesWithObfuscatedSecrets)
    verify(exactly = 1) { connectorBuilderService.updateBuilderProjectTestingValues(project.builderProjectId, newTestingValuesWithSecretCoordinates) }
  }

  @Test
  fun testReadStreamWithNoExistingTestingValues() {
    val project = generateBuilderProject()

    testStreamReadForProject(project, emptyObject())
  }

  @Test
  fun testReadStreamWithExistingTestingValues() {
    val project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates)
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    testStreamReadForProject(project, testingValues)
  }

  private fun testStreamReadForProject(
    project: ConnectorBuilderProject,
    testingValues: JsonNode,
  ) {
    val streamName = "stream1"
    val projectStreamReadRequestBody =
      ConnectorBuilderProjectStreamReadRequestBody(
        builderProjectId = project.builderProjectId,
        manifest = project.getManifestDraft(),
        streamName = streamName,
        workspaceId = project.workspaceId,
        formGeneratedManifest = false,
        recordLimit = null,
        pageLimit = null,
        sliceLimit = null,
      )

    val record1 =
      deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": 1,
            "name": "Bob"
          }
        }
        """.trimIndent(),
      )
    val record2 =
      deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": 2,
            "name": "Alice"
          }
        }
        """.trimIndent(),
      )
    val responseBody = "[" + serialize<JsonNode?>(record1) + "," + serialize<JsonNode?>(record2) + "]"
    val requestUrl = "https://api.com/users"
    val responseStatus = 200

    val expectedProjectStreamRead =
      ConnectorBuilderProjectStreamRead(
        logs = emptyList(),
        slices =
          listOf(
            ConnectorBuilderProjectStreamReadSlicesInner(
              pages =
                listOf(
                  ConnectorBuilderProjectStreamReadSlicesInnerPagesInner(
                    records = listOf(record1, record2),
                    request =
                      ConnectorBuilderHttpRequest(
                        url = requestUrl,
                        httpMethod = ConnectorBuilderHttpRequest.HttpMethod.GET,
                      ),
                    response = ConnectorBuilderHttpResponse(status = responseStatus, body = responseBody),
                  ),
                ),
            ),
          ),
        testReadLimitReached = false,
      )

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project
    every {
      manifestServerProcessor.streamTestRead(
        testingValues,
        project.getManifestDraft(),
        streamName,
        null,
        false,
        project.builderProjectId,
        null,
        null,
        null,
        mutableListOf(),
        project.workspaceId,
      )
    } returns expectedProjectStreamRead

    val actualProjectStreamRead =
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody)
    Assertions.assertEquals(expectedProjectStreamRead, actualProjectStreamRead)
  }

  @Test
  fun testReadStreamUpdatesPersistedTestingValues() {
    val spec = deserialize(specString)
    val project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates)
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    val streamName = "stream1"
    val projectStreamReadRequestBody =
      ConnectorBuilderProjectStreamReadRequestBody(
        builderProjectId = project.builderProjectId,
        manifest = project.getManifestDraft(),
        streamName = streamName,
        state = emptyList(),
        workspaceId = project.workspaceId,
        formGeneratedManifest = false,
        recordLimit = null,
        pageLimit = null,
        sliceLimit = null,
      )

    val newTestingValues =
      deserialize(
        """
        {
          "username": "alice",
          "password": "hunter3"
        }
        """.trimIndent(),
      )
    val newTestingValuesWithSecretCoordinates =
      deserialize(
        """
        {
          "username": "alice",
          "password": {
            "_secret": "airbyte_workspace_123_secret_456_v2"
          }
        }
        """.trimIndent(),
      )
    val newTestingValuesWithObfuscatedSecrets =
      deserialize(
        """
        {
          "username": "alice",
          "password": "**********"
        }
        """.trimIndent(),
      )

    val expectedStreamRead =
      ConnectorBuilderProjectStreamRead(
        logs = mutableListOf(),
        slices = mutableListOf(),
        testReadLimitReached = false,
        latestConfigUpdate = newTestingValues,
      )

    every { connectorBuilderService.getConnectorBuilderProject(project.builderProjectId, false) } returns project
    every {
      manifestServerProcessor.streamTestRead(
        testingValues,
        project.getManifestDraft(),
        streamName,
        null,
        false,
        project.builderProjectId,
        null,
        null,
        null,
        mutableListOf(),
        project.workspaceId,
      )
    } returns expectedStreamRead
    every {
      secretsRepositoryWriter.updateFromConfigLegacy(
        workspaceId,
        testingValuesWithSecretCoordinates,
        newTestingValues,
        spec,
        null,
      )
    } returns newTestingValuesWithSecretCoordinates
    every { secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec) } returns newTestingValuesWithObfuscatedSecrets

    val projectStreamRead =
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody)

    Assertions.assertEquals(newTestingValuesWithObfuscatedSecrets, projectStreamRead.latestConfigUpdate)
    verify(exactly = 1) { connectorBuilderService.updateBuilderProjectTestingValues(project.builderProjectId, newTestingValuesWithSecretCoordinates) }
  }

  @Test
  fun testGetBaseImageForDeclarativeManifest() {
    val requestBody = DeclarativeManifestRequestBody(manifest = A_MANIFEST)

    every { manifestInjector.getCdkVersion(any()) } returns A_CDK_VERSION
    every {
      declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
        any(),
      )
    } returns A_DECLARATIVE_MANIFEST_IMAGE_VERSION

    val responseBody = connectorBuilderProjectsHandler.getDeclarativeManifestBaseImage(requestBody)
    Assertions.assertEquals(A_BASE_IMAGE, responseBody.baseImage)
  }

  @Test
  fun testGetConnectorBuilderProjectIdBySourceDefinitionId() {
    val actorDefinitionId = UUID.randomUUID()
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId) } returns Optional.of(projectId)
    every { connectorBuilderService.getConnectorBuilderProject(projectId, false) } returns
      ConnectorBuilderProject().withBuilderProjectId(projectId).withWorkspaceId(workspaceId)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody(
          actorDefinitionId = actorDefinitionId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(projectId, response.builderProjectId)
    Assertions.assertEquals(workspaceId, response.workspaceId)
  }

  @Test
  fun testGetConnectorBuilderProjectIdBySourceDefinitionIdWhenNotFound() {
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId) } returns Optional.empty<UUID>()

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody(
          actorDefinitionId = actorDefinitionId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertNull(response.builderProjectId)
    Assertions.assertNull(response.workspaceId)
  }

  @Test
  fun testGetConnectorBuilderProjectIdBySourceDefinitionIdWhenProjectNotFound() {
    val actorDefinitionId = UUID.randomUUID()
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId) } returns Optional.of(projectId)
    every { connectorBuilderService.getConnectorBuilderProject(projectId, false) } returns nullAs()

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody(
          actorDefinitionId = actorDefinitionId,
          workspaceId = workspaceId,
        ),
      )

    Assertions.assertEquals(projectId, response.builderProjectId)
    Assertions.assertNull(response.workspaceId)
  }

  // the real service never returns null, but the handler tolerates it, and this test exercises that path
  @Suppress("UNCHECKED_CAST")
  private fun <T> nullAs(): T = null as T

  private fun setupConnectorSpecificationAdapter(
    spec: JsonNode,
    documentationUrl: String,
  ) {
    every { manifestInjector.createDeclarativeManifestConnectorSpecification(spec) } returns adaptedConnectorSpecification
    every { adaptedConnectorSpecification.documentationUrl } returns URI.create(documentationUrl)
  }

  private fun addSpec(manifest: JsonNode): JsonNode {
    val spec = deserialize("{\"" + ConnectorBuilderProjectsHandler.Companion.CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}")
    return (clone(manifest) as ObjectNode).set(ConnectorBuilderProjectsHandler.Companion.SPEC_FIELD, spec)
  }

  @Test
  fun testCreateForkedConnectorBuilderProjectActorDefinitionIdNotFound() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody(
        workspaceId = workspaceId,
        baseActorDefinitionId = baseActorDefinitionId,
      )
    every { sourceService.getStandardSourceDefinition(baseActorDefinitionId) } throws
      ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, baseActorDefinitionId)

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) { connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody) }
  }

  @Test
  fun testCreateForkedConnectorBuilderProjectManifestNotFound() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody(
        workspaceId = workspaceId,
        baseActorDefinitionId = baseActorDefinitionId,
      )

    val defaultVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-test"
    val dockerImageTag = "1.2.3"
    val defaultADV =
      ActorDefinitionVersion()
        .withActorDefinitionId(baseActorDefinitionId)
        .withDockerRepository(dockerRepository)
        .withDockerImageTag(dockerImageTag)
    every { sourceService.getStandardSourceDefinition(baseActorDefinitionId) } returns
      StandardSourceDefinition().withSourceDefinitionId(baseActorDefinitionId).withDefaultVersionId(defaultVersionId)
    every { actorDefinitionService.getActorDefinitionVersion(defaultVersionId) } returns defaultADV
    every { remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag) } returns Optional.empty<JsonNode>()

    Assertions.assertThrows(
      NotFoundException::class.java,
    ) { connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody) }
  }

  @Test
  fun testCreateForkedConnectorBuilderProject() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody(
        workspaceId = workspaceId,
        baseActorDefinitionId = baseActorDefinitionId,
      )

    val connectorName = "Test Connector"
    val baseActorDefinitionVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-test"
    val dockerImageTag = "1.2.3"
    val defaultADV =
      ActorDefinitionVersion()
        .withVersionId(baseActorDefinitionVersionId)
        .withActorDefinitionId(baseActorDefinitionId)
        .withDockerRepository(dockerRepository)
        .withDockerImageTag(dockerImageTag)
    val connectorBuilderProjectId = UUID.randomUUID()

    every { sourceService.getStandardSourceDefinition(baseActorDefinitionId) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(baseActorDefinitionId)
        .withDefaultVersionId(baseActorDefinitionVersionId)
        .withName(connectorName)
    every { actorDefinitionService.getActorDefinitionVersion(baseActorDefinitionVersionId) } returns defaultADV
    every { remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag) } returns Optional.of<JsonNode>(draftManifest)
    every { uuidSupplier.get() } returns connectorBuilderProjectId

    connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody)

    verify(exactly = 1) {
      connectorBuilderService.writeBuilderProjectDraft(
        connectorBuilderProjectId,
        workspaceId,
        connectorName,
        draftManifest,
        null,
        baseActorDefinitionVersionId,
        null,
        null,
      )
    }
  }

  @Test
  fun testCreateForkedConnectorBuilderProjectWithComponents() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody(
        workspaceId = workspaceId,
        baseActorDefinitionId = baseActorDefinitionId,
      )

    val connectorName = "Test Connector"
    val baseActorDefinitionVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-test"
    val dockerImageTag = "1.2.3"
    val defaultADV =
      ActorDefinitionVersion()
        .withVersionId(baseActorDefinitionVersionId)
        .withActorDefinitionId(baseActorDefinitionId)
        .withDockerRepository(dockerRepository)
        .withDockerImageTag(dockerImageTag)
    val connectorBuilderProjectId = UUID.randomUUID()

    every { sourceService.getStandardSourceDefinition(baseActorDefinitionId) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(baseActorDefinitionId)
        .withDefaultVersionId(baseActorDefinitionVersionId)
        .withName(connectorName)
    every { actorDefinitionService.getActorDefinitionVersion(baseActorDefinitionVersionId) } returns defaultADV
    every { remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag) } returns Optional.of<JsonNode>(draftManifest)
    every { remoteDefinitionsProvider.getConnectorCustomComponents(dockerRepository, dockerImageTag) } returns
      Optional.of<String>(A_CUSTOM_COMPONENTS_FILE_CONTENT)
    every { uuidSupplier.get() } returns connectorBuilderProjectId

    connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody)

    verify(exactly = 1) {
      connectorBuilderService.writeBuilderProjectDraft(
        connectorBuilderProjectId,
        workspaceId,
        connectorName,
        draftManifest,
        A_CUSTOM_COMPONENTS_FILE_CONTENT,
        baseActorDefinitionVersionId,
        null,
        null,
      )
    }
  }

  @Test
  fun testGetConnectorBuilderProjectOAuthConsent() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val consentUrl = "https://consent.url"

    val oAuthConfigSpecification = OAuthConfigSpecification()
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withManifestDraft(jsonNode(mapOf("spec" to spec)))
        .withTestingValues(testingValuesWithSecretCoordinates)
    every { connectorBuilderService.getConnectorBuilderProject(projectId, true) } returns project
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    val oAuthFlowImplementation: DeclarativeOAuthFlow = mockk<DeclarativeOAuthFlow>()
    every {
      oAuthFlowImplementation.getSourceConsentUrl(
        workspaceId,
        null,
        redirectUrl,
        testingValuesWithSecretCoordinates,
        any(),
        testingValues,
      )
    } returns consentUrl

    every {
      oauthImplementationFactory.createDeclarativeOAuthImplementation(
        any(),
      )
    } returns oAuthFlowImplementation

    val request =
      BuilderProjectOauthConsentRequest(
        builderProjectId = projectId,
        workspaceId = workspaceId,
        redirectUrl = redirectUrl,
      )

    val response = connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(request)

    verify(exactly = 1) {
      oAuthFlowImplementation.getSourceConsentUrl(
        workspaceId,
        null,
        redirectUrl,
        testingValuesWithSecretCoordinates,
        any(),
        testingValues,
      )
    }

    Assertions.assertEquals(consentUrl, response.consentUrl)
  }

  @Test
  fun testGetConnectorBuilderProjectOAuthConsentDoesNotExposeSecrets() {
    val consentUrlTemplate =
      "https://provider.com/auth?{{client_id_param}}&a={{ client_secret_value }}&b={{ client_secret }}&c={{ api_key }}&s={{ subdomain }}"

    val response = getOAuthConsentWithRealFlow(consentUrlTemplate)

    Assertions.assertTrue(response.consentUrl.startsWith("https://provider.com/auth?client_id=the-client-id&"), response.consentUrl)
    Assertions.assertFalse(response.consentUrl.contains("client-secret-value"), response.consentUrl)
    Assertions.assertFalse(response.consentUrl.contains("api-key-value"), response.consentUrl)
    Assertions.assertTrue(response.consentUrl.endsWith("&s=acme"), response.consentUrl)
  }

  @Test
  fun testGetConnectorBuilderProjectOAuthConsentDoesNotExposeSecretsTheDraftSpecNoLongerFlags() {
    listOf(setOf("client_id", "client_secret"), emptySet()).forEach { secretFields ->
      val response = getOAuthConsentWithRealFlow("https://provider.com/auth?{{client_id_param}}&c={{ api_key }}", secretFields)

      Assertions.assertEquals("https://provider.com/auth?client_id=the-client-id&c=", response.consentUrl)
    }
  }

  /**
   * The testing values are stored while client_id, client_secret and api_key are flagged as secrets; [secretFields] are the
   * fields the draft spec flags when the consent URL is requested.
   */
  private fun getOAuthConsentWithRealFlow(
    consentUrlTemplate: String,
    secretFields: Set<String> = setOf("client_id", "client_secret", "api_key"),
  ): OAuthConsentRead {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val connectionSpecification =
      jsonNode(
        mapOf(
          "type" to "object",
          "properties" to
            listOf("client_id", "client_secret", "api_key", "subdomain").associateWith { field ->
              if (field in secretFields) mapOf("type" to "string", "airbyte_secret" to true) else mapOf("type" to "string")
            },
        ),
      )
    val manifest =
      jsonNode(
        mapOf(
          "spec" to
            mapOf(
              "connection_specification" to connectionSpecification,
              "advanced_auth" to
                mapOf(
                  "oauth_config_specification" to
                    mapOf(
                      "oauth_connector_input_specification" to mapOf("consent_url" to consentUrlTemplate),
                      "complete_oauth_output_specification" to
                        mapOf(
                          "type" to "object",
                          "properties" to
                            mapOf(
                              "access_token" to
                                mapOf(
                                  "type" to "string",
                                  "path_in_connector_config" to listOf("access_token"),
                                  "path_in_oauth_response" to listOf("access_token"),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
      )
    val storedTestingValues =
      jsonNode(
        mapOf(
          "client_id" to mapOf("_secret" to "airbyte_workspace_123_secret_1_v1"),
          "client_secret" to mapOf("_secret" to "airbyte_workspace_123_secret_2_v1"),
          "api_key" to mapOf("_secret" to "airbyte_workspace_123_secret_3_v1"),
          "subdomain" to "acme",
        ),
      )
    val hydratedTestingValues =
      jsonNode(mapOf("client_id" to "the-client-id", "client_secret" to "client-secret-value", "api_key" to "api-key-value", "subdomain" to "acme"))

    every { connectorBuilderService.getConnectorBuilderProject(projectId, true) } returns
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withManifestDraft(manifest)
        .withTestingValues(storedTestingValues)
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(storedTestingValues) } returns hydratedTestingValues
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } answers {
      JsonSecretsProcessor(true).prepareSecretsForOutput(firstArg(), secondArg())
    }
    every { oauthImplementationFactory.createDeclarativeOAuthImplementation(any()) } returns DeclarativeOAuthFlow(HttpClient.newHttpClient())

    return connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(
      BuilderProjectOauthConsentRequest(
        builderProjectId = projectId,
        workspaceId = workspaceId,
        redirectUrl = "https://airbyte.com/auth_flow",
      ),
    )
  }

  @Test
  fun testCompleteConnectorBuilderProjectOAuth() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val queryParams = mapOf("code" to "12345")
    val oAuthResponse = mapOf("accessToken" to "token")

    val oAuthConfigSpecification = OAuthConfigSpecification()
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withManifestDraft(jsonNode(mapOf("spec" to spec)))
        .withTestingValues(testingValuesWithSecretCoordinates)
    every { connectorBuilderService.getConnectorBuilderProject(projectId, true) } returns project
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    val oAuthFlowMock: DeclarativeOAuthFlow = mockk<DeclarativeOAuthFlow>()
    every {
      oAuthFlowMock.completeSourceOAuth(
        workspaceId,
        null,
        queryParams,
        redirectUrl,
        testingValues,
        any(),
        testingValues,
      )
    } returns oAuthResponse

    every { oauthImplementationFactory.createDeclarativeOAuthImplementation(any()) } returns oAuthFlowMock

    every {
      connectorBuilderService.getConnectorBuilderProject(
        projectId,
        true,
      )
    } returns project

    val request =
      CompleteConnectorBuilderProjectOauthRequest(
        builderProjectId = projectId,
        workspaceId = workspaceId,
        queryParams = queryParams,
        redirectUrl = redirectUrl,
      )

    val response = connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(request)
    val expectedResponse = CompleteOAuthResponse(requestSucceeded = true, authPayload = oAuthResponse)
    verify(exactly = 1) {
      oAuthFlowMock.completeSourceOAuth(
        workspaceId,
        null,
        queryParams,
        redirectUrl,
        testingValues,
        any(),
        testingValues,
      )
    }

    Assertions.assertEquals(expectedResponse, response)
  }

  @Test
  fun testGetConnectorBuilderProjectOAuthConsentWithNullManifestDraft() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val consentUrl = "https://consent.url"

    val oAuthConfigSpecification = OAuthConfigSpecification()
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))

    // Mock project with null manifestDraft but with actorDefinitionId
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withActorDefinitionId(actorDefinitionId)
        .withManifestDraft(null)
        .withTestingValues(testingValuesWithSecretCoordinates)

    // Mock the published manifest
    val publishedManifest = jsonNode(mapOf("spec" to spec))
    val declarativeManifest =
      DeclarativeManifest()
        .withManifest(publishedManifest)
        .withActorDefinitionId(actorDefinitionId)

    every { connectorBuilderService.getConnectorBuilderProject(projectId, true) } returns project
    every { connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId) } returns declarativeManifest
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    val oAuthFlowImplementation: DeclarativeOAuthFlow = mockk<DeclarativeOAuthFlow>()
    every {
      oAuthFlowImplementation.getSourceConsentUrl(
        workspaceId,
        null,
        redirectUrl,
        testingValuesWithSecretCoordinates,
        any(),
        testingValues,
      )
    } returns consentUrl

    every {
      oauthImplementationFactory.createDeclarativeOAuthImplementation(
        any(),
      )
    } returns oAuthFlowImplementation

    val request =
      BuilderProjectOauthConsentRequest(
        builderProjectId = projectId,
        workspaceId = workspaceId,
        redirectUrl = redirectUrl,
      )

    val response = connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(request)

    // Verify that the published manifest was fetched
    verify(exactly = 1) { connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId) }

    verify(exactly = 1) {
      oAuthFlowImplementation.getSourceConsentUrl(
        workspaceId,
        null,
        redirectUrl,
        testingValuesWithSecretCoordinates,
        any(),
        testingValues,
      )
    }

    Assertions.assertEquals(consentUrl, response.consentUrl)
  }

  @Test
  fun testCompleteConnectorBuilderProjectOAuthWithNullManifestDraft() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val queryParams = mapOf("code" to "12345")
    val oAuthResponse = mapOf("accessToken" to "token")

    val oAuthConfigSpecification = OAuthConfigSpecification()
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))

    // Mock project with null manifestDraft but with actorDefinitionId
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withActorDefinitionId(actorDefinitionId)
        .withManifestDraft(null)
        .withTestingValues(testingValuesWithSecretCoordinates)

    // Mock the published manifest
    val publishedManifest = jsonNode(mapOf("spec" to spec))
    val declarativeManifest =
      DeclarativeManifest()
        .withManifest(publishedManifest)
        .withActorDefinitionId(actorDefinitionId)

    every { connectorBuilderService.getConnectorBuilderProject(projectId, true) } returns project
    every { connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId) } returns declarativeManifest
    every { secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates) } returns testingValues

    val oAuthFlowMock: DeclarativeOAuthFlow = mockk<DeclarativeOAuthFlow>()
    every {
      oAuthFlowMock.completeSourceOAuth(
        workspaceId,
        null,
        queryParams,
        redirectUrl,
        testingValues,
        any(),
        testingValues,
      )
    } returns oAuthResponse

    every { oauthImplementationFactory.createDeclarativeOAuthImplementation(any()) } returns oAuthFlowMock

    val request =
      CompleteConnectorBuilderProjectOauthRequest(
        builderProjectId = projectId,
        workspaceId = workspaceId,
        queryParams = queryParams,
        redirectUrl = redirectUrl,
      )

    val response = connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(request)
    val expectedResponse = CompleteOAuthResponse(requestSucceeded = true, authPayload = oAuthResponse)

    // Verify that the published manifest was fetched
    verify(exactly = 1) { connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId) }

    verify(exactly = 1) {
      oAuthFlowMock.completeSourceOAuth(
        workspaceId,
        null,
        queryParams,
        redirectUrl,
        testingValues,
        any(),
        testingValues,
      )
    }

    Assertions.assertEquals(expectedResponse, response)
  }

  @Test
  fun whenManifestHasFileUploaderThenReturnTrue() {
    val manifestWithFileUploader =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "test_stream", "file_uploader": {"type": "FileUploader"}}]}""",
      )
    Assertions.assertTrue(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithFileUploader))
  }

  @Test
  fun whenManifestHasNoFileUploaderThenReturnFalse() {
    val manifestWithoutFileUploader =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "test_stream"}]}""",
      )
    Assertions.assertFalse(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithoutFileUploader))
  }

  @Test
  fun whenManifestHasNoStreamsThenReturnFalse() {
    val manifestWithoutStreams = ObjectMapper().readTree("""{"version": "0.1.0"}""")
    Assertions.assertFalse(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithoutStreams))
  }

  @Test
  fun whenManifestHasNullFileUploaderThenReturnFalse() {
    val manifestWithNullFileUploader =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "test_stream", "file_uploader": null}]}""",
      )
    Assertions.assertFalse(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithNullFileUploader))
  }

  @Test
  fun whenManifestHasDynamicStreamsWithFileUploaderThenReturnTrue() {
    val manifestWithDynamicStreams =
      ObjectMapper().readTree(
        """{"dynamic_streams": [{"type": "DynamicDeclarativeStream", "name": "dynamic_stream", "file_uploader": {"type": "FileUploader"}}]}""",
      )
    Assertions.assertTrue(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithDynamicStreams))
  }

  @Test
  fun whenManifestHasFileUploaderInOneOfManyStreamsThenReturnTrue() {
    val manifestWithMixedStreams =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "no_upload"}, {"type": "DeclarativeStream", "name": "with_upload", "file_uploader": {"type": "FileUploader"}}]}""",
      )
    Assertions.assertTrue(ConnectorBuilderProjectsHandler.manifestHasFileUploader(manifestWithMixedStreams))
  }

  @Test
  fun whenPublishConnectorBuilderProjectWithFileUploaderThenSupportsFileTransferIsTrue() {
    val fileUploaderManifest =
      ObjectMapper().readTree(
        """{"streams": [{"type": "DeclarativeStream", "name": "file_stream", "file_uploader": {"type": "FileUploader"}}]}""",
      )

    every { uuidSupplier.get() } returns A_SOURCE_DEFINITION_ID
    every {
      manifestInjector.getManifestConnectorInjections(
        A_SOURCE_DEFINITION_ID,
        fileUploaderManifest,
        any(),
      )
    } returns listOf(A_CONFIG_INJECTION)
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL)

    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId)
    every { connectorBuilderService.getConnectorBuilderProject(any(), any()) } returns project

    val organizationId = UUID.randomUUID()
    every { workspaceService.getOrganizationIdFromWorkspaceId(any()) } returns Optional.of(organizationId)

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest(
        builderProjectId = A_BUILDER_PROJECT_ID,
        workspaceId = workspaceId,
        name = A_SOURCE_NAME,
        initialManifest = anyInitialManifest().copy(manifest = fileUploaderManifest),
      ),
    )

    verify(exactly = 1) {
      sourceService.writeCustomConnectorMetadata(
        any(),
        ActorDefinitionVersion()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
          .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.imageVersion)
          .withSpec(adaptedConnectorSpecification)
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withDocumentationUrl(A_DOCUMENTATION_URL)
          .withProtocolVersion("0.2.0")
          .withSupportsFileTransfer(true),
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }
  }

  companion object {
    private val A_SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val A_BUILDER_PROJECT_ID: UUID = UUID.randomUUID()
    private val A_WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val A_VERSION = 32L
    private const val ACTIVE_MANIFEST_VERSION = 865L
    private val A_CDK_VERSION = Version("0.0.1")
    private val A_DECLARATIVE_MANIFEST_IMAGE_VERSION =
      DeclarativeManifestImageVersion(
        0,
        "0.79.0",
        "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c",
        OffsetDateTime.now(),
        OffsetDateTime.now(),
      )
    private const val A_BASE_IMAGE =
      "docker.io/airbyte/source-declarative-manifest:0.79.0@sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"
    private const val A_DESCRIPTION = "a description"
    private const val A_SOURCE_NAME = "a source name"
    private const val A_NAME = "a name"
    private const val A_DOCUMENTATION_URL = "http://documentation.url"
    private val A_MANIFEST: JsonNode
    private val A_SPEC: JsonNode
    private val A_CONFIG_INJECTION: ActorDefinitionConfigInjection = ActorDefinitionConfigInjection().withInjectionPath("something")
    private const val A_PULL_REQUEST_URL = "https://github.com/airbytehq/airbyte/pull/44579"
    private val A_CONTRIBUTION_ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val A_CUSTOM_COMPONENTS_FILE_CONTENT = "custom components file content"

    private val forkedSourceDefinitionId: UUID = UUID.randomUUID()
    private val FORKED_ADV: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(forkedSourceDefinitionId)
        .withDockerRepository("airbyte/source-test")
        .withDockerImageTag("0.1.0")
        .withDocumentationUrl("https://documentation.com")
    private val FORKED_SOURCE: StandardSourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(forkedSourceDefinitionId)
        .withName("A test source")
        .withIconUrl("https://icon.com")

    init {
      try {
        A_MANIFEST = ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}")
        A_SPEC = ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }

    private fun anyConnectorBuilderProjectRequest(
      builderProjectId: UUID,
      workspaceId: UUID,
      name: String = "",
      initialManifest: DeclarativeSourceManifest = anyInitialManifest(),
    ): ConnectorBuilderPublishRequestBody =
      ConnectorBuilderPublishRequestBody(
        workspaceId = workspaceId,
        builderProjectId = builderProjectId,
        name = name,
        initialDeclarativeManifest = initialManifest,
      )

    private fun anyInitialManifest(): DeclarativeSourceManifest =
      DeclarativeSourceManifest(description = A_DESCRIPTION, manifest = A_MANIFEST, spec = A_SPEC, version = A_VERSION)
  }
}
