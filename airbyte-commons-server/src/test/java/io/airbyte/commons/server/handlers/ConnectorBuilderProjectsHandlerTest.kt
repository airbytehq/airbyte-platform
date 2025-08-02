/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.BaseActorDefinitionVersionInfo
import io.airbyte.api.model.generated.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.model.generated.BuilderProjectOauthConsentRequest
import io.airbyte.api.model.generated.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.model.generated.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.model.generated.DeclarativeManifestRequestBody
import io.airbyte.api.model.generated.DeclarativeSourceManifest
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.constants.AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
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
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner
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
import io.airbyte.metrics.MetricClient
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import java.io.IOException
import java.net.URI
import java.time.OffsetDateTime
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Stream

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
  private lateinit var connectorBuilderServerApiClient: ConnectorBuilderServerApi
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var adaptedConnectorSpecification: ConnectorSpecification
  private lateinit var oauthImplementationFactory: OAuthImplementationFactory
  private lateinit var metricClient: MetricClient
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
  @Throws(JsonProcessingException::class)
  fun setUp() {
    declarativeManifestImageVersionService =
      Mockito.mock(DeclarativeManifestImageVersionService::class.java)
    connectorBuilderService = Mockito.mock(ConnectorBuilderService::class.java)
    builderProjectUpdater = Mockito.mock(BuilderProjectUpdater::class.java)
    uuidSupplier = Mockito.mock(Supplier::class.java) as Supplier<UUID>
    manifestInjector = Mockito.mock(DeclarativeSourceManifestInjector::class.java)
    workspaceService = Mockito.mock(WorkspaceService::class.java)
    featureFlagClient = Mockito.mock(TestClient::class.java)
    secretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    secretsRepositoryWriter = Mockito.mock(SecretsRepositoryWriter::class.java)
    secretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)
    sourceService = Mockito.mock(SourceService::class.java)
    secretsProcessor = Mockito.mock(JsonSecretsProcessor::class.java)
    connectorBuilderServerApiClient = Mockito.mock(ConnectorBuilderServerApi::class.java)
    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    remoteDefinitionsProvider = Mockito.mock(RemoteDefinitionsProvider::class.java)
    adaptedConnectorSpecification = Mockito.mock(ConnectorSpecification::class.java)
    oauthImplementationFactory = Mockito.mock(OAuthImplementationFactory::class.java)
    metricClient = Mockito.mock(MetricClient::class.java)
    setupConnectorSpecificationAdapter(anyOrNull(), "")
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
        connectorBuilderServerApiClient,
        actorDefinitionService,
        remoteDefinitionsProvider,
        oauthImplementationFactory,
        metricClient,
      )

    Mockito.`when`(manifestInjector.getCdkVersion(anyOrNull())).thenReturn(A_CDK_VERSION)
    Mockito
      .`when`(
        declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
          anyOrNull(),
        ),
      ).thenReturn(A_DECLARATIVE_MANIFEST_IMAGE_VERSION)
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
  @Throws(IOException::class)
  fun testCreateConnectorBuilderProject() {
    val project = generateBuilderProject()

    Mockito.`when`(uuidSupplier.get()).thenReturn(project.getBuilderProjectId())

    val create =
      ConnectorBuilderProjectWithWorkspaceId()
        .builderProject(
          ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()),
        ).workspaceId(workspaceId)

    val response = connectorBuilderProjectsHandler.createConnectorBuilderProject(create)
    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProjectId())
    Assertions.assertEquals(project.getWorkspaceId(), response.getWorkspaceId())

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .writeBuilderProjectDraft(
        project.getBuilderProjectId(),
        project.getWorkspaceId(),
        project.getName(),
        project.getManifestDraft(),
        null,
        project.getBaseActorDefinitionVersionId(),
        project.getContributionPullRequestUrl(),
        project.getContributionActorDefinitionId(),
      )
  }

  @Test
  @DisplayName("publishConnectorBuilderProject throws a helpful error if no associated CDK version is found")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testCreateConnectorBuilderProjectNoCdkVersion() {
    val project = generateBuilderProject()
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(project)

    Mockito.`when`(uuidSupplier.get()).thenReturn(project.getBuilderProjectId())
    Mockito
      .`when`(
        declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
          anyOrNull(),
        ),
      ).thenThrow(IllegalStateException("No declarative manifest image version found in database for major version 0"))

    val publish =
      ConnectorBuilderPublishRequestBody()
        .name("")
        .builderProjectId(project.getBuilderProjectId())
        .workspaceId(workspaceId)
        .initialDeclarativeManifest(DeclarativeSourceManifest().spec(A_SPEC).manifest(A_MANIFEST))

    Assertions.assertEquals(
      "No declarative manifest image version found in database for major version 0",
      Assertions
        .assertThrows(
          IllegalStateException::class.java,
          Executable { connectorBuilderProjectsHandler.publishConnectorBuilderProject(publish) },
        ).message,
    )
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdateConnectorBuilderProject() {
    val project = generateBuilderProject()

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(
          ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()),
        ).workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId())

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(update)

    Mockito
      .verify(builderProjectUpdater, Mockito.times(1))
      .persistBuilderProjectUpdate(
        update,
      )
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should validate whether the workspace does not match")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testUpdateConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    val wrongWorkspace = UUID.randomUUID()

    val update =
      ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(
          ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()),
        ).workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId())

    project.setWorkspaceId(wrongWorkspace)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          project.getBuilderProjectId(),
          false,
        ),
      ).thenReturn(project)

    Assertions.assertThrows(ConfigNotFoundException::class.java) {
      connectorBuilderProjectsHandler.updateConnectorBuilderProject(update)
    }

    Mockito.verify(connectorBuilderService, Mockito.never()).writeBuilderProjectDraft(
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
    )
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testDeleteConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    val wrongWorkspace = UUID.randomUUID()

    project.setWorkspaceId(wrongWorkspace)
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
      Executable {
        connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
          ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
        )
      },
    )

    Mockito
      .verify(connectorBuilderService, Mockito.never())
      .deleteBuilderProject(anyOrNull())
  }

  @Test
  @DisplayName("publishBuilderProject should validate whether the workspace does not match")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testPublishConnectorBuilderProjectValidateWorkspace() {
    val project = generateBuilderProject()
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    val wrongWorkspaceId = UUID.randomUUID()
    val manifest: DeclarativeSourceManifest? =
      anyInitialManifest()!!.manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(
        A_DESCRIPTION,
      )
    val publishReq: ConnectorBuilderPublishRequestBody =
      anyConnectorBuilderProjectRequest()
        .builderProjectId(project.getBuilderProjectId())
        .workspaceId(wrongWorkspaceId)
        .initialDeclarativeManifest(manifest)

    Mockito.`when`(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID)

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
      Executable { connectorBuilderProjectsHandler.publishConnectorBuilderProject(publishReq) },
    )
    Mockito.verify(connectorBuilderService, Mockito.never()).insertActiveDeclarativeManifest(
      anyOrNull(),
    )
    Mockito.verify(connectorBuilderService, Mockito.never()).assignActorDefinitionToConnectorBuilderProject(
      anyOrNull(),
      anyOrNull(),
    )
    Mockito
      .verify(connectorBuilderService, Mockito.never())
      .deleteBuilderProjectDraft(anyOrNull())
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should delete an existing project")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testDeleteConnectorBuilderProject() {
    val project = generateBuilderProject()

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)

    connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
      ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
    )

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .deleteBuilderProject(
        project.getBuilderProjectId(),
      )
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list all projects without drafts")
  @Throws(IOException::class)
  fun testListConnectorBuilderProject() {
    val project1 = generateBuilderProject()
    val project2 = generateBuilderProject()
    project2.setHasDraft(false)
    project1.setActiveDeclarativeManifestVersion(A_VERSION)
    project1.setActorDefinitionId(UUID.randomUUID())

    Mockito.`when`(connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId!!)).thenReturn(
      Stream.of(project1, project2),
    )

    val response =
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(WorkspaceIdRequestBody().workspaceId(workspaceId))

    Assertions.assertEquals(project1.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId())
    Assertions.assertEquals(project2.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId())

    Assertions.assertTrue(response.getProjects().get(0).getHasDraft())
    Assertions.assertFalse(response.getProjects().get(1).getHasDraft())

    Assertions.assertEquals(project1.getActiveDeclarativeManifestVersion(), response.getProjects().get(0).getActiveDeclarativeManifestVersion())
    Assertions.assertEquals(project1.getActorDefinitionId(), response.getProjects().get(0).getSourceDefinitionId())
    Assertions.assertNull(project2.getActiveDeclarativeManifestVersion())
    Assertions.assertNull(project2.getActorDefinitionId())

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .getConnectorBuilderProjectsByWorkspace(
        workspaceId!!,
      )
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list both forked and non-forked projects")
  @Throws(IOException::class)
  fun testListForkedAndNonForkedProjects() {
    val unforkedProject = generateBuilderProject()
    val forkedProject = generateBuilderProject().withActorDefinitionId(UUID.randomUUID())
    forkedProject.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId())

    Mockito.`when`(connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId!!)).thenReturn(
      Stream.of(unforkedProject, forkedProject),
    )
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersions(java.util.List.of(FORKED_ADV.getVersionId())))
      .thenReturn(
        java.util.List.of(
          FORKED_ADV,
        ),
      )
    Mockito
      .`when`(sourceService.listStandardSourceDefinitions(false))
      .thenReturn(
        java.util.List.of(
          StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()),
          FORKED_SOURCE,
        ),
      )

    val response =
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(WorkspaceIdRequestBody().workspaceId(workspaceId))

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo()
        .name(FORKED_SOURCE.getName())
        .dockerRepository(FORKED_ADV.getDockerRepository())
        .dockerImageTag(FORKED_ADV.getDockerImageTag())
        .actorDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .icon(FORKED_SOURCE.getIconUrl())
        .documentationUrl(FORKED_ADV.getDocumentationUrl())

    Assertions.assertEquals(2, response.getProjects().size)
    Assertions.assertEquals(unforkedProject.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId())
    Assertions.assertEquals(forkedProject.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId())
    Assertions.assertNull(response.getProjects().get(0).getBaseActorDefinitionVersionInfo())
    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.getProjects().get(1).getBaseActorDefinitionVersionInfo())
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
  fun testGetConnectorBuilderProject() {
    val project = generateBuilderProject()
    project.setActorDefinitionId(UUID.randomUUID())
    project.setActiveDeclarativeManifestVersion(A_VERSION)
    project.setTestingValues(testingValuesWithSecretCoordinates)

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)))
      .thenReturn(testingValuesWithObfuscatedSecrets)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertEquals(project.getActorDefinitionId(), response.getBuilderProject().getSourceDefinitionId())
    Assertions.assertEquals(project.getActiveDeclarativeManifestVersion(), response.getBuilderProject().getActiveDeclarativeManifestVersion())
    Assertions.assertTrue(response.getDeclarativeManifest().getIsDraft())
    Assertions.assertEquals(
      serialize<JsonNode?>(draftManifest),
      ObjectMapper().writeValueAsString(response.getDeclarativeManifest().getManifest()),
    )
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues())
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and null testing values if it doesn't have any")
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
  fun testGetConnectorBuilderProjectNullTestingValues() {
    val project = generateBuilderProject()
    project.setActorDefinitionId(UUID.randomUUID())
    project.setActiveDeclarativeManifestVersion(A_VERSION)
    project.setTestingValues(null)

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertEquals(project.getActorDefinitionId(), response.getBuilderProject().getSourceDefinitionId())
    Assertions.assertEquals(project.getActiveDeclarativeManifestVersion(), response.getBuilderProject().getActiveDeclarativeManifestVersion())
    Assertions.assertTrue(response.getDeclarativeManifest().getIsDraft())
    Assertions.assertEquals(
      serialize<JsonNode?>(draftManifest),
      ObjectMapper().writeValueAsString(response.getDeclarativeManifest().getManifest()),
    )
    Assertions.assertNull(response.getTestingValues())
    Mockito
      .verify(secretsProcessor, Mockito.never())
      .prepareSecretsForOutput(anyOrNull(), anyOrNull())
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testGetConnectorBuilderProjectWithoutDraft() {
    val project = generateBuilderProject()
    project.setManifestDraft(null)
    project.setHasDraft(false)
    project.setTestingValues(null)

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertFalse(response.getBuilderProject().getHasDraft())
    Assertions.assertNull(response.getDeclarativeManifest())
    Assertions.assertNull(response.getTestingValues())
    Mockito
      .verify(secretsProcessor, Mockito.never())
      .prepareSecretsForOutput(anyOrNull(), anyOrNull())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testGetConnectorBuilderProjectWithBaseActorDefinitionVersion() {
    val project = generateBuilderProject()
    project.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId())

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)
    Mockito.`when`(actorDefinitionService.getActorDefinitionVersion(FORKED_ADV.getVersionId())).thenReturn(FORKED_ADV)
    Mockito.`when`(sourceService.getStandardSourceDefinition(FORKED_SOURCE.getSourceDefinitionId())).thenReturn(
      FORKED_SOURCE,
    )

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    val expectedBaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo()
        .name(FORKED_SOURCE.getName())
        .dockerRepository(FORKED_ADV.getDockerRepository())
        .dockerImageTag(FORKED_ADV.getDockerImageTag())
        .actorDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .icon(FORKED_SOURCE.getIconUrl())
        .documentationUrl(FORKED_ADV.getDocumentationUrl())

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertEquals(expectedBaseActorDefinitionVersionInfo, response.getBuilderProject().getBaseActorDefinitionVersionInfo())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testGetConnectorBuilderProjectWithContributionInfo() {
    val project = generateBuilderProject()
    project.setContributionPullRequestUrl(A_PULL_REQUEST_URL)
    project.setContributionActorDefinitionId(A_CONTRIBUTION_ACTOR_DEFINITION_ID)

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertEquals(A_PULL_REQUEST_URL, response.getBuilderProject().getContributionInfo().getPullRequestUrl())
    Assertions.assertEquals(A_CONTRIBUTION_ACTOR_DEFINITION_ID, response.getBuilderProject().getContributionInfo().getActorDefinitionId())
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun givenNoVersionButActiveManifestWhenGetConnectorBuilderProjectWithManifestThenReturnActiveVersion() {
    val project =
      generateBuilderProject()
        .withManifestDraft(null)
        .withHasDraft(false)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withTestingValues(testingValuesWithSecretCoordinates)
    val manifest = addSpec(A_MANIFEST)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(project.getBuilderProjectId()),
          anyOrNull(),
        ),
      ).thenReturn(project)
    Mockito
      .`when`(
        connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
          A_SOURCE_DEFINITION_ID,
        ),
      ).thenReturn(
        DeclarativeManifest()
          .withManifest(manifest)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION),
      )
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)))
      .thenReturn(testingValuesWithObfuscatedSecrets)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId),
      )

    Assertions.assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId())
    Assertions.assertFalse(response.getBuilderProject().getHasDraft())
    Assertions.assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion())
    Assertions.assertEquals(manifest, response.getDeclarativeManifest().getManifest())
    Assertions.assertEquals(false, response.getDeclarativeManifest().getIsDraft())
    Assertions.assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription())
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun givenVersionWhenGetConnectorBuilderProjectWithManifestThenReturnSpecificVersion() {
    val manifest = addSpec(A_MANIFEST)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(A_BUILDER_PROJECT_ID),
          eq(false),
        ),
      ).thenReturn(
        ConnectorBuilderProject().withWorkspaceId(A_WORKSPACE_ID),
      )
    Mockito
      .`when`(
        connectorBuilderService.getVersionedConnectorBuilderProject(
          eq(A_BUILDER_PROJECT_ID),
          eq(A_VERSION),
        ),
      ).thenReturn(
        ConnectorBuilderProjectVersionedManifest()
          .withBuilderProjectId(A_BUILDER_PROJECT_ID)
          .withActiveDeclarativeManifestVersion(ACTIVE_MANIFEST_VERSION)
          .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
          .withHasDraft(true)
          .withName(A_NAME)
          .withManifest(manifest)
          .withManifestVersion(A_VERSION)
          .withManifestDescription(A_DESCRIPTION)
          .withTestingValues(testingValuesWithSecretCoordinates),
      )
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, deserialize(specString)))
      .thenReturn(testingValuesWithObfuscatedSecrets)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(A_BUILDER_PROJECT_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION),
      )

    Assertions.assertEquals(A_BUILDER_PROJECT_ID, response.getBuilderProject().getBuilderProjectId())
    Assertions.assertEquals(A_NAME, response.getBuilderProject().getName())
    Assertions.assertEquals(ACTIVE_MANIFEST_VERSION, response.getBuilderProject().getActiveDeclarativeManifestVersion())
    Assertions.assertEquals(A_SOURCE_DEFINITION_ID, response.getBuilderProject().getSourceDefinitionId())
    Assertions.assertEquals(true, response.getBuilderProject().getHasDraft())
    Assertions.assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion())
    Assertions.assertEquals(manifest, response.getDeclarativeManifest().getManifest())
    Assertions.assertEquals(false, response.getDeclarativeManifest().getIsDraft())
    Assertions.assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription())
    Assertions.assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun whenPublishConnectorBuilderProjectThenReturnActorDefinition() {
    Mockito.`when`(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID)
    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(project)

    val req: ConnectorBuilderPublishRequestBody =
      anyConnectorBuilderProjectRequest()
        .builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(A_WORKSPACE_ID)
        .initialDeclarativeManifest(anyInitialManifest()!!.spec(emptyObject()).manifest(emptyObject()))
    val response = connectorBuilderProjectsHandler.publishConnectorBuilderProject(req)
    Assertions.assertEquals(A_SOURCE_DEFINITION_ID, response.getSourceDefinitionId())
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun whenPublishConnectorBuilderProjectThenCreateActorDefinition() {
    Mockito.`when`(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID)
    Mockito
      .`when`(
        manifestInjector.getManifestConnectorInjections(
          A_SOURCE_DEFINITION_ID,
          A_MANIFEST,
          null,
        ),
      ).thenReturn(
        listOf(
          A_CONFIG_INJECTION,
        ),
      )
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL)

    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(project)

    val organizationId = UUID.randomUUID()
    Mockito
      .`when`(
        workspaceService.getOrganizationIdFromWorkspaceId(
          anyOrNull(),
        ),
      ).thenReturn(Optional.of(organizationId))

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest()
        .builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(workspaceId)
        .name(A_SOURCE_NAME)
        .initialDeclarativeManifest(anyInitialManifest()!!.manifest(A_MANIFEST).spec(A_SPEC)),
    )

    Mockito
      .verify(manifestInjector, Mockito.times(1))
      .addInjectedDeclarativeManifest(
        A_SPEC,
      )
    Mockito
      .verify(sourceService, Mockito.times(1))
      .writeCustomConnectorMetadata(
        eq(
          StandardSourceDefinition()
            .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .withName(A_SOURCE_NAME)
            .withSourceType(StandardSourceDefinition.SourceType.CUSTOM)
            .withTombstone(false)
            .withPublic(false)
            .withCustom(true),
        ),
        eq(
          ActorDefinitionVersion()
            .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
            .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
            .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.imageVersion)
            .withSpec(adaptedConnectorSpecification)
            .withSupportLevel(SupportLevel.NONE)
            .withInternalSupportLevel(100L)
            .withReleaseStage(ReleaseStage.CUSTOM)
            .withDocumentationUrl(A_DOCUMENTATION_URL)
            .withProtocolVersion("0.2.0"),
        ),
        eq(organizationId),
        eq(ScopeType.ORGANIZATION),
      )

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .writeActorDefinitionConfigInjectionsForPath(
//                eq(A_SOURCE_DEFINITION_ID),
        eq(listOf(A_CONFIG_INJECTION)),
      )
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun whenNoOrganizationFoundShouldUseWorkspaceScope() {
    Mockito.`when`(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID)
    Mockito
      .`when`(
        manifestInjector.getManifestConnectorInjections(
          A_SOURCE_DEFINITION_ID,
          A_MANIFEST,
          null,
        ),
      ).thenReturn(
        listOf(
          A_CONFIG_INJECTION,
        ),
      )
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL)

    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId)
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(anyOrNull(), anyOrNull()))
      .thenReturn(project)

    Mockito
      .`when`(workspaceService.getOrganizationIdFromWorkspaceId(anyOrNull()))
      .thenReturn(Optional.empty<UUID>())

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest()
        .builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(workspaceId)
        .name(A_SOURCE_NAME)
        .initialDeclarativeManifest(anyInitialManifest()!!.manifest(A_MANIFEST).spec(A_SPEC)),
    )

    Mockito
      .verify(manifestInjector, Mockito.times(1))
      .addInjectedDeclarativeManifest(
        A_SPEC,
      )
    Mockito
      .verify(sourceService, Mockito.times(1))
      .writeCustomConnectorMetadata(
        eq(
          StandardSourceDefinition()
            .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .withName(A_SOURCE_NAME)
            .withSourceType(StandardSourceDefinition.SourceType.CUSTOM)
            .withTombstone(false)
            .withPublic(false)
            .withCustom(true),
        ),
        eq(
          ActorDefinitionVersion()
            .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
            .withDockerRepository(AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
            .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.imageVersion)
            .withSpec(adaptedConnectorSpecification)
            .withSupportLevel(SupportLevel.NONE)
            .withInternalSupportLevel(100L)
            .withReleaseStage(ReleaseStage.CUSTOM)
            .withDocumentationUrl(A_DOCUMENTATION_URL)
            .withProtocolVersion("0.2.0"),
        ),
        eq(workspaceId),
        eq(ScopeType.WORKSPACE),
      )

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .writeActorDefinitionConfigInjectionsForPath(
//                eq(A_SOURCE_DEFINITION_ID),
        eq(listOf(A_CONFIG_INJECTION)),
      )
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() {
    Mockito.`when`(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID)
    val project = generateBuilderProject().withWorkspaceId(A_WORKSPACE_ID)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(project)

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest()
        .builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(A_WORKSPACE_ID)
        .initialDeclarativeManifest(
          anyInitialManifest()!!
            .manifest(A_MANIFEST)
            .spec(A_SPEC)
            .version(A_VERSION)
            .description(A_DESCRIPTION),
        ),
    )

    Mockito.verify(connectorBuilderService, Mockito.times(1)).insertActiveDeclarativeManifest(
      eq(
        DeclarativeManifest()
          .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
          .withVersion(A_VERSION)
          .withDescription(A_DESCRIPTION)
          .withManifest(A_MANIFEST)
          .withSpec(A_SPEC),
      ),
    )
    Mockito.verify(connectorBuilderService, Mockito.times(1)).assignActorDefinitionToConnectorBuilderProject(
      A_BUILDER_PROJECT_ID,
      A_SOURCE_DEFINITION_ID,
    )
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun whenPublishConnectorBuilderProjectThenDraftDeleted() {
    val project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID)
    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(project)
    Mockito.`when`(uuidSupplier.get()).thenReturn(project.getBuilderProjectId())

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      anyConnectorBuilderProjectRequest()
        .builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(A_WORKSPACE_ID)
        .initialDeclarativeManifest(
          anyInitialManifest()!!
            .manifest(A_MANIFEST)
            .spec(A_SPEC)
            .version(A_VERSION)
            .description(A_DESCRIPTION),
        ),
    )

    Mockito.verify(connectorBuilderService, Mockito.times(1)).deleteBuilderProjectDraft(A_BUILDER_PROJECT_ID)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
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

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)
    Mockito
      .`when`(secretsRepositoryWriter.createFromConfigLegacy(workspaceId!!, testingValues, spec, null))
      .thenReturn(testingValuesWithSecretCoordinates)
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, spec))
      .thenReturn(testingValuesWithObfuscatedSecrets)

    val response =
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        ConnectorBuilderProjectTestingValuesUpdate()
          .workspaceId(workspaceId)
          .builderProjectId(project.getBuilderProjectId())
          .testingValues(testingValues)
          .spec(spec),
      )
    Assertions.assertEquals(response, testingValuesWithObfuscatedSecrets)
    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .updateBuilderProjectTestingValues(project.getBuilderProjectId(), testingValuesWithSecretCoordinates)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
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

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates))
      .thenReturn(testingValues)
    Mockito.`when`(secretsProcessor.copySecrets(testingValues, newTestingValues, spec)).thenReturn(newTestingValues)
    Mockito
      .`when`(
        secretsRepositoryWriter.updateFromConfigLegacy(
          workspaceId!!,
          testingValuesWithSecretCoordinates,
          newTestingValues,
          spec,
          null,
        ),
      ).thenReturn(newTestingValuesWithSecretCoordinates)
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec))
      .thenReturn(testingValuesWithObfuscatedSecrets)

    val response =
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        ConnectorBuilderProjectTestingValuesUpdate()
          .builderProjectId(project.getBuilderProjectId())
          .workspaceId(workspaceId)
          .testingValues(newTestingValues)
          .spec(spec),
      )
    Assertions.assertEquals(response, testingValuesWithObfuscatedSecrets)
    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .updateBuilderProjectTestingValues(project.getBuilderProjectId(), newTestingValuesWithSecretCoordinates)
  }

  @Test
  @Throws(Exception::class)
  fun testReadStreamWithNoExistingTestingValues() {
    val project = generateBuilderProject()

    testStreamReadForProject(project, emptyObject())
  }

  @Test
  @Throws(Exception::class)
  fun testReadStreamWithExistingTestingValues() {
    val project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates))
      .thenReturn(testingValues)

    testStreamReadForProject(project, testingValues)
  }

  @Throws(Exception::class)
  private fun testStreamReadForProject(
    project: ConnectorBuilderProject,
    testingValues: JsonNode,
  ) {
    val streamName = "stream1"
    val projectStreamReadRequestBody =
      ConnectorBuilderProjectStreamReadRequestBody()
        .builderProjectId(project.getBuilderProjectId())
        .manifest(project.getManifestDraft())
        .streamName(streamName)
        .workspaceId(project.getWorkspaceId())
        .formGeneratedManifest(false)
        .recordLimit(null)
        .pageLimit(null)
        .sliceLimit(null)

    val streamReadRequestBody =
      StreamReadRequestBody(
        testingValues,
        project.getManifestDraft(),
        streamName,
        null,
        false,
        project.getBuilderProjectId().toString(),
        null,
        null,
        null,
        mutableListOf(),
        project.getWorkspaceId().toString(),
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
    val httpRequest = HttpRequest(requestUrl, HttpRequest.HttpMethod.GET, null, null)
    val httpResponse = HttpResponse(responseStatus, responseBody, null)
    val streamRead =
      StreamRead(
        mutableListOf(),
        java.util.List.of(
          StreamReadSlicesInner(
            java.util.List.of(
              StreamReadSlicesInnerPagesInner(
                java.util.List.of(record1, record2),
                httpRequest,
                httpResponse,
              ),
            ),
            null,
            null,
            mutableListOf(),
          ),
        ),
        false,
        null,
        null,
        null,
        null,
      )

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)
    Mockito.`when`(connectorBuilderServerApiClient.readStream(streamReadRequestBody)).thenReturn(streamRead)

    val expectedProjectStreamRead =
      ConnectorBuilderProjectStreamRead()
        .logs(mutableListOf<@Valid ConnectorBuilderProjectStreamReadLogsInner?>())
        .slices(
          java.util.List.of<@Valid ConnectorBuilderProjectStreamReadSlicesInner?>(
            ConnectorBuilderProjectStreamReadSlicesInner()
              .pages(
                java.util.List.of<@Valid ConnectorBuilderProjectStreamReadSlicesInnerPagesInner?>(
                  ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                    .records(java.util.List.of<Any?>(record1, record2))
                    .request(ConnectorBuilderHttpRequest().url(requestUrl).httpMethod(ConnectorBuilderHttpRequest.HttpMethodEnum.GET))
                    .response(ConnectorBuilderHttpResponse().status(responseStatus).body(responseBody)),
                ),
              ),
          ),
        ).testReadLimitReached(false)
    val actualProjectStreamRead =
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody)
    Assertions.assertEquals(expectedProjectStreamRead, actualProjectStreamRead)
  }

  @Test
  @Throws(Exception::class)
  fun testReadStreamUpdatesPersistedTestingValues() {
    val spec = deserialize(specString)
    val project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates))
      .thenReturn(testingValues)

    val streamName = "stream1"
    val projectStreamReadRequestBody =
      ConnectorBuilderProjectStreamReadRequestBody()
        .builderProjectId(project.getBuilderProjectId())
        .manifest(project.getManifestDraft())
        .streamName(streamName)
        .state(mutableListOf<Any?>())
        .workspaceId(project.getWorkspaceId())
        .formGeneratedManifest(false)
        .recordLimit(null)
        .pageLimit(null)
        .sliceLimit(null)

    val streamReadRequestBody =
      StreamReadRequestBody(
        testingValues,
        project.getManifestDraft(),
        streamName,
        null,
        false,
        project.getBuilderProjectId().toString(),
        null,
        null,
        null,
        mutableListOf(),
        project.getWorkspaceId().toString(),
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
    val streamRead =
      StreamRead(mutableListOf(), mutableListOf(), false, null, null, null, newTestingValues)

    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false))
      .thenReturn(project)
    Mockito.`when`(connectorBuilderServerApiClient.readStream(streamReadRequestBody)).thenReturn(streamRead)
    Mockito
      .`when`(
        secretsRepositoryWriter.updateFromConfigLegacy(
          workspaceId!!,
          testingValuesWithSecretCoordinates,
          newTestingValues,
          spec,
          null,
        ),
      ).thenReturn(newTestingValuesWithSecretCoordinates)
    Mockito
      .`when`(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec))
      .thenReturn(newTestingValuesWithObfuscatedSecrets)

    val projectStreamRead =
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody)

    Assertions.assertEquals(newTestingValuesWithObfuscatedSecrets, projectStreamRead.getLatestConfigUpdate())
    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .updateBuilderProjectTestingValues(project.getBuilderProjectId(), newTestingValuesWithSecretCoordinates)
  }

  @Test
  fun testGetBaseImageForDeclarativeManifest() {
    val requestBody = DeclarativeManifestRequestBody().manifest(A_MANIFEST)

    Mockito.`when`(manifestInjector.getCdkVersion(anyOrNull())).thenReturn(A_CDK_VERSION)
    Mockito
      .`when`(
        declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(
          anyOrNull(),
        ),
      ).thenReturn(A_DECLARATIVE_MANIFEST_IMAGE_VERSION)

    val responseBody = connectorBuilderProjectsHandler.getDeclarativeManifestBaseImage(requestBody)
    Assertions.assertEquals(A_BASE_IMAGE, responseBody.getBaseImage())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testGetConnectorBuilderProjectIdBySourceDefinitionId() {
    val actorDefinitionId = UUID.randomUUID()
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    Mockito.`when`(connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId)).thenReturn(
      Optional.of(projectId),
    )
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(projectId, false))
      .thenReturn(ConnectorBuilderProject().withBuilderProjectId(projectId).withWorkspaceId(workspaceId))

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody().actorDefinitionId(actorDefinitionId).workspaceId(workspaceId),
      )

    Assertions.assertEquals(projectId, response.getBuilderProjectId())
    Assertions.assertEquals(workspaceId, response.getWorkspaceId())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testGetConnectorBuilderProjectIdBySourceDefinitionIdWhenNotFound() {
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    Mockito.`when`(connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId)).thenReturn(
      Optional.empty<UUID>(),
    )

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody().actorDefinitionId(actorDefinitionId).workspaceId(workspaceId),
      )

    Assertions.assertNull(response.getBuilderProjectId())
    Assertions.assertNull(response.getWorkspaceId())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testGetConnectorBuilderProjectIdBySourceDefinitionIdWhenProjectNotFound() {
    val actorDefinitionId = UUID.randomUUID()
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    Mockito.`when`(connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId)).thenReturn(
      Optional.of(projectId),
    )
    Mockito.`when`(connectorBuilderService.getConnectorBuilderProject(projectId, false)).thenReturn(null)

    val response =
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        BuilderProjectForDefinitionRequestBody().actorDefinitionId(actorDefinitionId).workspaceId(workspaceId),
      )

    Assertions.assertEquals(projectId, response.getBuilderProjectId())
    Assertions.assertNull(response.getWorkspaceId())
  }

  private fun setupConnectorSpecificationAdapter(
    spec: JsonNode,
    documentationUrl: String,
  ) {
    Mockito
      .`when`(manifestInjector.createDeclarativeManifestConnectorSpecification(spec))
      .thenReturn(adaptedConnectorSpecification)
    Mockito.`when`(adaptedConnectorSpecification!!.getDocumentationUrl()).thenReturn(URI.create(documentationUrl))
  }

  private fun addSpec(manifest: JsonNode): JsonNode {
    val spec = deserialize("{\"" + ConnectorBuilderProjectsHandler.Companion.CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}")
    return (clone(manifest) as ObjectNode).set(ConnectorBuilderProjectsHandler.Companion.SPEC_FIELD, spec)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCreateForkedConnectorBuilderProjectActorDefinitionIdNotFound() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId)
    Mockito
      .`when`(sourceService.getStandardSourceDefinition(baseActorDefinitionId))
      .thenThrow(ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, baseActorDefinitionId))

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
      Executable { connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody) },
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCreateForkedConnectorBuilderProjectManifestNotFound() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId)

    val defaultVersionId = UUID.randomUUID()
    val dockerRepository = "airbyte/source-test"
    val dockerImageTag = "1.2.3"
    val defaultADV =
      ActorDefinitionVersion()
        .withActorDefinitionId(baseActorDefinitionId)
        .withDockerRepository(dockerRepository)
        .withDockerImageTag(dockerImageTag)
    Mockito
      .`when`(sourceService.getStandardSourceDefinition(baseActorDefinitionId))
      .thenReturn(StandardSourceDefinition().withSourceDefinitionId(baseActorDefinitionId).withDefaultVersionId(defaultVersionId))
    Mockito.`when`(actorDefinitionService.getActorDefinitionVersion(defaultVersionId)).thenReturn(defaultADV)
    Mockito
      .`when`(remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag))
      .thenReturn(Optional.empty<JsonNode>())

    Assertions.assertThrows(
      NotFoundException::class.java,
      Executable { connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody) },
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCreateForkedConnectorBuilderProject() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId)

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

    Mockito.`when`(sourceService.getStandardSourceDefinition(baseActorDefinitionId)).thenReturn(
      StandardSourceDefinition()
        .withSourceDefinitionId(baseActorDefinitionId)
        .withDefaultVersionId(baseActorDefinitionVersionId)
        .withName(connectorName),
    )
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(baseActorDefinitionVersionId))
      .thenReturn(defaultADV)
    Mockito
      .`when`(remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag))
      .thenReturn(Optional.of<JsonNode>(draftManifest))
    Mockito.`when`(uuidSupplier.get()).thenReturn(connectorBuilderProjectId)

    connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody)

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .writeBuilderProjectDraft(
        eq(connectorBuilderProjectId),
        eq(workspaceId),
        eq(connectorName),
        eq(draftManifest),
        eq(null),
        eq(baseActorDefinitionVersionId),
        eq(null),
        eq(null),
      )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCreateForkedConnectorBuilderProjectWithComponents() {
    val workspaceId = UUID.randomUUID()
    val baseActorDefinitionId = UUID.randomUUID()
    val requestBody =
      ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId)

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

    Mockito.`when`(sourceService.getStandardSourceDefinition(baseActorDefinitionId)).thenReturn(
      StandardSourceDefinition()
        .withSourceDefinitionId(baseActorDefinitionId)
        .withDefaultVersionId(baseActorDefinitionVersionId)
        .withName(connectorName),
    )
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(baseActorDefinitionVersionId))
      .thenReturn(defaultADV)
    Mockito
      .`when`(remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag))
      .thenReturn(Optional.of<JsonNode>(draftManifest))
    Mockito
      .`when`(remoteDefinitionsProvider.getConnectorCustomComponents(dockerRepository, dockerImageTag))
      .thenReturn(Optional.of<String>(A_CUSTOM_COMPONENTS_FILE_CONTENT))
    Mockito.`when`(uuidSupplier.get()).thenReturn(connectorBuilderProjectId)

    connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody)

    Mockito
      .verify(connectorBuilderService, Mockito.times(1))
      .writeBuilderProjectDraft(
        eq(connectorBuilderProjectId),
        eq(workspaceId),
        eq(connectorName),
        eq(draftManifest),
        eq(A_CUSTOM_COMPONENTS_FILE_CONTENT),
        eq(baseActorDefinitionVersionId),
        eq(null),
        eq(null),
      )
  }

  @Test
  @Throws(Exception::class)
  fun testGetConnectorBuilderProjectOAuthConsent() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val consentUrl = "https://consent.url"

    val oAuthConfigSpecification: OAuthConfigSpecification = Mockito.mock(OAuthConfigSpecification::class.java)
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withManifestDraft(jsonNode(Map.of("spec", spec)))
        .withTestingValues(testingValuesWithSecretCoordinates)
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(projectId, true))
      .thenReturn(project)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates))
      .thenReturn(testingValues)

    val oAuthFlowImplementation: DeclarativeOAuthFlow = Mockito.mock(DeclarativeOAuthFlow::class.java)
    Mockito
      .`when`(
        oAuthFlowImplementation.getSourceConsentUrl(
          eq(workspaceId),
          eq(null),
          eq(redirectUrl),
          eq(testingValues),
          anyOrNull(),
          eq(testingValues),
        ),
      ).thenReturn(consentUrl)

    Mockito
      .`when`(
        oauthImplementationFactory.createDeclarativeOAuthImplementation(
          anyOrNull(),
        ),
      ).thenReturn(oAuthFlowImplementation)

    val request =
      BuilderProjectOauthConsentRequest()
        .builderProjectId(projectId)
        .workspaceId(workspaceId)
        .redirectUrl(redirectUrl)

    val response = connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(request)

    Mockito
      .verify(oAuthFlowImplementation, Mockito.times(1))
      .getSourceConsentUrl(
        eq(workspaceId),
        eq(null),
        eq(redirectUrl),
        eq(testingValues),
        anyOrNull(),
        eq(testingValues),
      )

    Assertions.assertEquals(consentUrl, response.getConsentUrl())
  }

  @Test
  @Throws(Exception::class)
  fun testCompleteConnectorBuilderProjectOAuth() {
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val redirectUrl = "https://airbyte.com/auth_flow"
    val queryParams = Map.of<String, Any>("code", "12345")
    val oAuthResponse = Map.of<String, Any>("accessToken", "token")

    val oAuthConfigSpecification: OAuthConfigSpecification = Mockito.mock(OAuthConfigSpecification::class.java)
    val spec =
      ConnectorSpecification().withAdvancedAuth(AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification))
    val project =
      ConnectorBuilderProject()
        .withWorkspaceId(workspaceId)
        .withManifestDraft(jsonNode(Map.of("spec", spec)))
        .withTestingValues(testingValuesWithSecretCoordinates)
    Mockito
      .`when`(connectorBuilderService.getConnectorBuilderProject(projectId, true))
      .thenReturn(project)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates))
      .thenReturn(testingValues)

    val oAuthFlowMock: DeclarativeOAuthFlow = Mockito.mock(DeclarativeOAuthFlow::class.java)
    Mockito
      .`when`(
        oAuthFlowMock.completeSourceOAuth(
          eq(workspaceId),
          eq(null),
          eq(queryParams),
          eq(redirectUrl),
          eq(testingValues),
          anyOrNull(),
          eq(testingValues),
        ),
      ).thenReturn(oAuthResponse)

    Mockito
      .`when`(oauthImplementationFactory.createDeclarativeOAuthImplementation(anyOrNull()))
      .thenReturn(oAuthFlowMock)

    Mockito
      .`when`(
        connectorBuilderService.getConnectorBuilderProject(
          eq(projectId),
          eq(true),
        ),
      ).thenReturn(project)

    val request =
      CompleteConnectorBuilderProjectOauthRequest()
        .builderProjectId(projectId)
        .workspaceId(workspaceId)
        .queryParams(queryParams)
        .redirectUrl(redirectUrl)

    val response = connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(request)
    val expectedResponse = CompleteOAuthResponse().requestSucceeded(true).authPayload(oAuthResponse)
    Mockito
      .verify(oAuthFlowMock, Mockito.times(1))
      .completeSourceOAuth(
        eq(workspaceId),
        eq(null),
        eq(queryParams),
        eq(redirectUrl),
        eq(testingValues),
        anyOrNull(),
        eq(testingValues),
      )

    Assertions.assertEquals(expectedResponse, response)
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

    private fun anyConnectorBuilderProjectRequest(): ConnectorBuilderPublishRequestBody =
      ConnectorBuilderPublishRequestBody()
        .name("")
        .initialDeclarativeManifest(anyInitialManifest())

    private fun anyInitialManifest(): DeclarativeSourceManifest? = DeclarativeSourceManifest().version(A_VERSION)
  }
}
