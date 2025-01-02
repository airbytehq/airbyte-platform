/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.CONNECTION_SPECIFICATION_FIELD;
import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.SPEC_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.BaseActorDefinitionVersionInfo;
import io.airbyte.api.model.generated.BuilderProjectForDefinitionRequestBody;
import io.airbyte.api.model.generated.BuilderProjectForDefinitionResponse;
import io.airbyte.api.model.generated.BuilderProjectOauthConsentRequest;
import io.airbyte.api.model.generated.CompleteConnectorBuilderProjectOauthRequest;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest;
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest.HttpMethodEnum;
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails;
import io.airbyte.api.model.generated.ConnectorBuilderProjectForkRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner;
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.DeclarativeManifestBaseImageRead;
import io.airbyte.api.model.generated.DeclarativeManifestRequestBody;
import io.airbyte.api.model.generated.DeclarativeSourceManifest;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.OAuthConsentRead;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest.HttpMethod;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DeclarativeManifestImageVersionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.oauth.OAuthImplementationFactory;
import io.airbyte.oauth.declarative.DeclarativeOAuthFlow;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class ConnectorBuilderProjectsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final Long A_VERSION = 32L;
  private static final Long ACTIVE_MANIFEST_VERSION = 865L;
  private static final Version A_CDK_VERSION = new Version("0.0.1");
  private static final DeclarativeManifestImageVersion A_DECLARATIVE_MANIFEST_IMAGE_VERSION =
      new DeclarativeManifestImageVersion(0, "0.79.0", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c",
          OffsetDateTime.now(), OffsetDateTime.now());
  private static final String A_BASE_IMAGE =
      "docker.io/airbyte/source-declarative-manifest:0.79.0@sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c";
  private static final String A_DESCRIPTION = "a description";
  private static final String A_SOURCE_NAME = "a source name";
  private static final String A_NAME = "a name";
  private static final String A_DOCUMENTATION_URL = "http://documentation.url";
  private static final JsonNode A_MANIFEST;
  private static final JsonNode A_SPEC;
  private static final ActorDefinitionConfigInjection A_CONFIG_INJECTION = new ActorDefinitionConfigInjection().withInjectionPath("something");
  private static final String A_PULL_REQUEST_URL = "https://github.com/airbytehq/airbyte/pull/44579";
  private static final UUID A_CONTRIBUTION_ACTOR_DEFINITION_ID = UUID.randomUUID();

  private static final UUID forkedSourceDefinitionId = UUID.randomUUID();
  private static final ActorDefinitionVersion FORKED_ADV = new ActorDefinitionVersion()
      .withVersionId(UUID.randomUUID())
      .withActorDefinitionId(forkedSourceDefinitionId)
      .withDockerRepository("airbyte/source-test")
      .withDockerImageTag("0.1.0")
      .withDocumentationUrl("https://documentation.com");
  private static final StandardSourceDefinition FORKED_SOURCE = new StandardSourceDefinition()
      .withSourceDefinitionId(forkedSourceDefinitionId)
      .withName("A test source")
      .withIconUrl("https://icon.com");

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private DeclarativeManifestImageVersionService declarativeManifestImageVersionService;
  private ConnectorBuilderService connectorBuilderService;
  private BuilderProjectUpdater builderProjectUpdater;
  private ConnectorBuilderProjectsHandler connectorBuilderProjectsHandler;
  private Supplier<UUID> uuidSupplier;
  private DeclarativeSourceManifestInjector manifestInjector;
  private WorkspaceService workspaceService;
  private FeatureFlagClient featureFlagClient;
  private SecretsRepositoryReader secretsRepositoryReader;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private SourceService sourceService;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorBuilderServerApi connectorBuilderServerApiClient;
  private ActorDefinitionService actorDefinitionService;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private ConnectorSpecification adaptedConnectorSpecification;
  private OAuthImplementationFactory oauthImplementationFactory;
  private UUID workspaceId;
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
  private final JsonNode draftManifest = addSpec(Jsons.deserialize("{\"test\":123,\"empty\":{\"array_in_object\":[]}}"));
  private final JsonNode testingValues = Jsons.deserialize(
      """
      {
        "username": "bob",
        "password": "hunter2"
      }""");
  private final JsonNode testingValuesWithSecretCoordinates = Jsons.deserialize(
      """
      {
        "username": "bob",
        "password": {
          "_secret": "airbyte_workspace_123_secret_456_v1"
        }
      }""");
  private final JsonNode testingValuesWithObfuscatedSecrets = Jsons.deserialize(
      """
      {
        "username": "bob",
        "password": "**********"
      }""");

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws JsonProcessingException {
    declarativeManifestImageVersionService = mock(DeclarativeManifestImageVersionService.class);
    connectorBuilderService = mock(ConnectorBuilderService.class);
    builderProjectUpdater = mock(BuilderProjectUpdater.class);
    uuidSupplier = mock(Supplier.class);
    manifestInjector = mock(DeclarativeSourceManifestInjector.class);
    workspaceService = mock(WorkspaceService.class);
    featureFlagClient = mock(TestClient.class);
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    sourceService = mock(SourceService.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    connectorBuilderServerApiClient = mock(ConnectorBuilderServerApi.class);
    actorDefinitionService = mock(ActorDefinitionService.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);
    oauthImplementationFactory = mock(OAuthImplementationFactory.class);
    setupConnectorSpecificationAdapter(any(), "");
    workspaceId = UUID.randomUUID();

    connectorBuilderProjectsHandler =
        new ConnectorBuilderProjectsHandler(declarativeManifestImageVersionService, connectorBuilderService, builderProjectUpdater, uuidSupplier,
            manifestInjector,
            workspaceService, featureFlagClient,
            secretsRepositoryReader, secretsRepositoryWriter, secretPersistenceConfigService, sourceService, secretsProcessor,
            connectorBuilderServerApiClient, actorDefinitionService, remoteDefinitionsProvider, oauthImplementationFactory);

    when(manifestInjector.getCdkVersion(any())).thenReturn(A_CDK_VERSION);
    when(declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(anyInt()))
        .thenReturn(A_DECLARATIVE_MANIFEST_IMAGE_VERSION);
  }

  private ConnectorBuilderProject generateBuilderProject() {
    final UUID projectId = UUID.randomUUID();
    return new ConnectorBuilderProject().withBuilderProjectId(projectId).withWorkspaceId(workspaceId).withName("Test project")
        .withHasDraft(true).withManifestDraft(draftManifest);
  }

  @Test
  @DisplayName("createConnectorBuilderProject should create a new project and return the id")
  void testCreateConnectorBuilderProject() throws IOException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(uuidSupplier.get()).thenReturn(project.getBuilderProjectId());

    final ConnectorBuilderProjectWithWorkspaceId create = new ConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId);

    final ConnectorBuilderProjectIdWithWorkspaceId response = connectorBuilderProjectsHandler.createConnectorBuilderProject(create);
    assertEquals(project.getBuilderProjectId(), response.getBuilderProjectId());
    assertEquals(project.getWorkspaceId(), response.getWorkspaceId());

    verify(connectorBuilderService, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), project.getManifestDraft(),
            project.getBaseActorDefinitionVersionId(), project.getContributionPullRequestUrl(), project.getContributionActorDefinitionId());
  }

  @Test
  @DisplayName("publishConnectorBuilderProject throws a helpful error if no associated CDK version is found")
  void testCreateConnectorBuilderProjectNoCdkVersion() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    when(connectorBuilderService.getConnectorBuilderProject(any(UUID.class), any(boolean.class))).thenReturn(project);

    when(uuidSupplier.get()).thenReturn(project.getBuilderProjectId());
    when(declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(anyInt()))
        .thenThrow(new IllegalStateException("No declarative manifest image version found in database for major version 0"));

    final ConnectorBuilderPublishRequestBody publish = new ConnectorBuilderPublishRequestBody()
        .builderProjectId(project.getBuilderProjectId())
        .workspaceId(workspaceId)
        .initialDeclarativeManifest(new DeclarativeSourceManifest().spec(A_SPEC).manifest(A_MANIFEST));

    assertEquals("No declarative manifest image version found in database for major version 0",
        assertThrows(IllegalStateException.class, () -> connectorBuilderProjectsHandler.publishConnectorBuilderProject(publish)).getMessage());
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should update an existing project")
  void testUpdateConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId());

    connectorBuilderProjectsHandler.updateConnectorBuilderProject(update);

    verify(builderProjectUpdater, times(1))
        .persistBuilderProjectUpdate(
            update);
  }

  @Test
  @DisplayName("updateConnectorBuilderProject should validate whether the workspace does not match")
  void testUpdateConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final UUID wrongWorkspace = UUID.randomUUID();

    final ExistingConnectorBuilderProjectWithWorkspaceId update = new ExistingConnectorBuilderProjectWithWorkspaceId()
        .builderProject(new ConnectorBuilderProjectDetails()
            .name(project.getName())
            .draftManifest(project.getManifestDraft()))
        .workspaceId(workspaceId)
        .builderProjectId(project.getBuilderProjectId());

    project.setWorkspaceId(wrongWorkspace);
    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.updateConnectorBuilderProject(update));

    verify(connectorBuilderService, never()).writeBuilderProjectDraft(any(UUID.class), any(UUID.class), any(String.class), any(JsonNode.class),
        any(UUID.class), any(String.class), any(UUID.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  void testDeleteConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final UUID wrongWorkspace = UUID.randomUUID();

    project.setWorkspaceId(wrongWorkspace);
    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId)));

    verify(connectorBuilderService, never()).deleteBuilderProject(any(UUID.class));
  }

  @Test
  @DisplayName("publishBuilderProject should validate whether the workspace does not match")
  void testPublishConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    final UUID wrongWorkspaceId = UUID.randomUUID();
    final DeclarativeSourceManifest manifest = anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION);
    final ConnectorBuilderPublishRequestBody publishReq = anyConnectorBuilderProjectRequest()
        .builderProjectId(project.getBuilderProjectId())
        .workspaceId(wrongWorkspaceId)
        .initialDeclarativeManifest(manifest);

    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.publishConnectorBuilderProject(publishReq));
    verify(connectorBuilderService, never()).insertActiveDeclarativeManifest(any(DeclarativeManifest.class));
    verify(connectorBuilderService, never()).assignActorDefinitionToConnectorBuilderProject(any(UUID.class), any(UUID.class));
    verify(connectorBuilderService, never()).deleteBuilderProjectDraft(any(UUID.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should delete an existing project")
  void testDeleteConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    verify(connectorBuilderService, times(1))
        .deleteBuilderProject(
            project.getBuilderProjectId());
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list all projects without drafts")
  void testListConnectorBuilderProject() throws IOException {
    final ConnectorBuilderProject project1 = generateBuilderProject();
    final ConnectorBuilderProject project2 = generateBuilderProject();
    project2.setHasDraft(false);
    project1.setActiveDeclarativeManifestVersion(A_VERSION);
    project1.setActorDefinitionId(UUID.randomUUID());

    when(connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId)).thenReturn(Stream.of(project1, project2));

    final ConnectorBuilderProjectReadList response =
        connectorBuilderProjectsHandler.listConnectorBuilderProjects(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(project1.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId());
    assertEquals(project2.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId());

    assertTrue(response.getProjects().get(0).getHasDraft());
    assertFalse(response.getProjects().get(1).getHasDraft());

    assertEquals(project1.getActiveDeclarativeManifestVersion(), response.getProjects().get(0).getActiveDeclarativeManifestVersion());
    assertEquals(project1.getActorDefinitionId(), response.getProjects().get(0).getSourceDefinitionId());
    assertNull(project2.getActiveDeclarativeManifestVersion());
    assertNull(project2.getActorDefinitionId());

    verify(connectorBuilderService, times(1))
        .getConnectorBuilderProjectsByWorkspace(
            workspaceId);
  }

  @Test
  @DisplayName("listConnectorBuilderProject should list both forked and non-forked projects")
  void testListForkedAndNonForkedProjects() throws IOException {
    final ConnectorBuilderProject unforkedProject = generateBuilderProject();
    final ConnectorBuilderProject forkedProject = generateBuilderProject().withActorDefinitionId(UUID.randomUUID());
    forkedProject.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId());

    when(connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId)).thenReturn(Stream.of(unforkedProject, forkedProject));
    when(actorDefinitionService.getActorDefinitionVersions(List.of(FORKED_ADV.getVersionId()))).thenReturn(List.of(FORKED_ADV));
    when(sourceService.listStandardSourceDefinitions(false))
        .thenReturn(List.of(new StandardSourceDefinition().withSourceDefinitionId(UUID.randomUUID()), FORKED_SOURCE));

    final ConnectorBuilderProjectReadList response =
        connectorBuilderProjectsHandler.listConnectorBuilderProjects(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final BaseActorDefinitionVersionInfo expectedBaseActorDefinitionVersionInfo = new BaseActorDefinitionVersionInfo()
        .name(FORKED_SOURCE.getName())
        .dockerRepository(FORKED_ADV.getDockerRepository())
        .dockerImageTag(FORKED_ADV.getDockerImageTag())
        .actorDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .icon(FORKED_SOURCE.getIconUrl())
        .documentationUrl(FORKED_ADV.getDocumentationUrl());

    assertEquals(2, response.getProjects().size());
    assertEquals(unforkedProject.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId());
    assertEquals(forkedProject.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId());
    assertNull(response.getProjects().get(0).getBaseActorDefinitionVersionInfo());
    assertEquals(expectedBaseActorDefinitionVersionInfo, response.getProjects().get(1).getBaseActorDefinitionVersionInfo());
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  void testGetConnectorBuilderProject() throws IOException, ConfigNotFoundException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setActorDefinitionId(UUID.randomUUID());
    project.setActiveDeclarativeManifestVersion(A_VERSION);
    project.setTestingValues(testingValuesWithSecretCoordinates);

    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
    when(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, Jsons.deserialize(specString)))
        .thenReturn(testingValuesWithObfuscatedSecrets);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertEquals(project.getActorDefinitionId(), response.getBuilderProject().getSourceDefinitionId());
    assertEquals(project.getActiveDeclarativeManifestVersion(), response.getBuilderProject().getActiveDeclarativeManifestVersion());
    assertTrue(response.getDeclarativeManifest().getIsDraft());
    assertEquals(Jsons.serialize(draftManifest), new ObjectMapper().writeValueAsString(response.getDeclarativeManifest().getManifest()));
    assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues());
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and null testing values if it doesn't have any")
  void testGetConnectorBuilderProjectNullTestingValues() throws IOException, ConfigNotFoundException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setActorDefinitionId(UUID.randomUUID());
    project.setActiveDeclarativeManifestVersion(A_VERSION);
    project.setTestingValues(null);

    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertEquals(project.getActorDefinitionId(), response.getBuilderProject().getSourceDefinitionId());
    assertEquals(project.getActiveDeclarativeManifestVersion(), response.getBuilderProject().getActiveDeclarativeManifestVersion());
    assertTrue(response.getDeclarativeManifest().getIsDraft());
    assertEquals(Jsons.serialize(draftManifest), new ObjectMapper().writeValueAsString(response.getDeclarativeManifest().getManifest()));
    assertNull(response.getTestingValues());
    verify(secretsProcessor, never()).prepareSecretsForOutput(any(), any());
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  void testGetConnectorBuilderProjectWithoutDraft() throws IOException, ConfigNotFoundException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setManifestDraft(null);
    project.setHasDraft(false);
    project.setTestingValues(null);

    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertFalse(response.getBuilderProject().getHasDraft());
    assertNull(response.getDeclarativeManifest());
    assertNull(response.getTestingValues());
    verify(secretsProcessor, never()).prepareSecretsForOutput(any(), any());
  }

  @Test
  void testGetConnectorBuilderProjectWithBaseActorDefinitionVersion() throws ConfigNotFoundException, IOException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setBaseActorDefinitionVersionId(FORKED_ADV.getVersionId());

    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
    when(actorDefinitionService.getActorDefinitionVersion(FORKED_ADV.getVersionId())).thenReturn(FORKED_ADV);
    when(sourceService.getStandardSourceDefinition(FORKED_SOURCE.getSourceDefinitionId())).thenReturn(FORKED_SOURCE);

    final ConnectorBuilderProjectRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    final BaseActorDefinitionVersionInfo expectedBaseActorDefinitionVersionInfo = new BaseActorDefinitionVersionInfo()
        .name(FORKED_SOURCE.getName())
        .dockerRepository(FORKED_ADV.getDockerRepository())
        .dockerImageTag(FORKED_ADV.getDockerImageTag())
        .actorDefinitionId(FORKED_SOURCE.getSourceDefinitionId())
        .icon(FORKED_SOURCE.getIconUrl())
        .documentationUrl(FORKED_ADV.getDocumentationUrl());

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertEquals(expectedBaseActorDefinitionVersionInfo, response.getBuilderProject().getBaseActorDefinitionVersionInfo());
  }

  @Test
  void testGetConnectorBuilderProjectWithContributionInfo() throws ConfigNotFoundException, IOException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setContributionPullRequestUrl(A_PULL_REQUEST_URL);
    project.setContributionActorDefinitionId(A_CONTRIBUTION_ACTOR_DEFINITION_ID);

    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

    final ConnectorBuilderProjectRead response =
        connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
            new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertEquals(A_PULL_REQUEST_URL, response.getBuilderProject().getContributionInfo().getPullRequestUrl());
    assertEquals(A_CONTRIBUTION_ACTOR_DEFINITION_ID, response.getBuilderProject().getContributionInfo().getActorDefinitionId());
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  void givenNoVersionButActiveManifestWhenGetConnectorBuilderProjectWithManifestThenReturnActiveVersion()
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject()
        .withManifestDraft(null)
        .withHasDraft(false)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withTestingValues(testingValuesWithSecretCoordinates);
    final JsonNode manifest = addSpec(A_MANIFEST);
    when(connectorBuilderService.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
    when(connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID))
        .thenReturn(new DeclarativeManifest()
            .withManifest(manifest)
            .withVersion(A_VERSION)
            .withDescription(A_DESCRIPTION));
    when(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, Jsons.deserialize(specString)))
        .thenReturn(testingValuesWithObfuscatedSecrets);

    final ConnectorBuilderProjectRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    assertEquals(project.getBuilderProjectId(), response.getBuilderProject().getBuilderProjectId());
    assertFalse(response.getBuilderProject().getHasDraft());
    assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion());
    assertEquals(manifest, response.getDeclarativeManifest().getManifest());
    assertEquals(false, response.getDeclarativeManifest().getIsDraft());
    assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription());
    assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues());
  }

  @Test
  void givenVersionWhenGetConnectorBuilderProjectWithManifestThenReturnSpecificVersion()
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final JsonNode manifest = addSpec(A_MANIFEST);
    when(connectorBuilderService.getConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(false))).thenReturn(
        new ConnectorBuilderProject().withWorkspaceId(A_WORKSPACE_ID));
    when(connectorBuilderService.getVersionedConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(A_VERSION))).thenReturn(
        new ConnectorBuilderProjectVersionedManifest()
            .withBuilderProjectId(A_BUILDER_PROJECT_ID)
            .withActiveDeclarativeManifestVersion(ACTIVE_MANIFEST_VERSION)
            .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
            .withHasDraft(true)
            .withName(A_NAME)
            .withManifest(manifest)
            .withManifestVersion(A_VERSION)
            .withManifestDescription(A_DESCRIPTION)
            .withTestingValues(testingValuesWithSecretCoordinates));
    when(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, Jsons.deserialize(specString)))
        .thenReturn(testingValuesWithObfuscatedSecrets);

    final ConnectorBuilderProjectRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(A_BUILDER_PROJECT_ID).workspaceId(A_WORKSPACE_ID).version(A_VERSION));

    assertEquals(A_BUILDER_PROJECT_ID, response.getBuilderProject().getBuilderProjectId());
    assertEquals(A_NAME, response.getBuilderProject().getName());
    assertEquals(ACTIVE_MANIFEST_VERSION, response.getBuilderProject().getActiveDeclarativeManifestVersion());
    assertEquals(A_SOURCE_DEFINITION_ID, response.getBuilderProject().getSourceDefinitionId());
    assertEquals(true, response.getBuilderProject().getHasDraft());
    assertEquals(A_VERSION, response.getDeclarativeManifest().getVersion());
    assertEquals(manifest, response.getDeclarativeManifest().getManifest());
    assertEquals(false, response.getDeclarativeManifest().getIsDraft());
    assertEquals(A_DESCRIPTION, response.getDeclarativeManifest().getDescription());
    assertEquals(testingValuesWithObfuscatedSecrets, response.getTestingValues());
  }

  @Test
  void whenPublishConnectorBuilderProjectThenReturnActorDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    final ConnectorBuilderProject project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID);
    when(connectorBuilderService.getConnectorBuilderProject(any(UUID.class), any(boolean.class))).thenReturn(project);

    final ConnectorBuilderPublishRequestBody req =
        anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID).workspaceId(A_WORKSPACE_ID);
    final SourceDefinitionIdBody response = connectorBuilderProjectsHandler.publishConnectorBuilderProject(req);
    assertEquals(A_SOURCE_DEFINITION_ID, response.getSourceDefinitionId());
  }

  @Test
  void whenPublishConnectorBuilderProjectThenCreateActorDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    when(manifestInjector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)).thenReturn(A_CONFIG_INJECTION);
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL);

    final ConnectorBuilderProject project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(workspaceId);
    when(connectorBuilderService.getConnectorBuilderProject(any(UUID.class), any(boolean.class))).thenReturn(project);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
        anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID).workspaceId(workspaceId).name(A_SOURCE_NAME)
            .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC)));

    verify(manifestInjector, times(1)).addInjectedDeclarativeManifest(A_SPEC);
    verify(sourceService, times(1)).writeCustomConnectorMetadata(eq(new StandardSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_SOURCE_NAME)
        .withSourceType(SourceType.CUSTOM)
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)), eq(
            new ActorDefinitionVersion()
                .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
                .withDockerRepository("airbyte/source-declarative-manifest")
                .withDockerImageTag(A_DECLARATIVE_MANIFEST_IMAGE_VERSION.getImageVersion())
                .withSpec(adaptedConnectorSpecification)
                .withSupportLevel(SupportLevel.NONE)
                .withInternalSupportLevel(100L)
                .withReleaseStage(ReleaseStage.CUSTOM)
                .withDocumentationUrl(A_DOCUMENTATION_URL)
                .withProtocolVersion("0.2.0")),
        eq(workspaceId),
        eq(ScopeType.WORKSPACE));
    verify(connectorBuilderService, times(1)).writeActorDefinitionConfigInjectionForPath(eq(A_CONFIG_INJECTION));
  }

  @Test
  void whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() throws ConfigNotFoundException, IOException, JsonValidationException {

    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    final ConnectorBuilderProject project = generateBuilderProject().withWorkspaceId(A_WORKSPACE_ID);
    when(connectorBuilderService.getConnectorBuilderProject(any(UUID.class), any(boolean.class))).thenReturn(project);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(A_WORKSPACE_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(connectorBuilderService, times(1)).insertActiveDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(connectorBuilderService, times(1)).assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
  }

  @Test
  void whenPublishConnectorBuilderProjectThenDraftDeleted() throws ConfigNotFoundException, IOException, JsonValidationException {
    final ConnectorBuilderProject project = generateBuilderProject().withBuilderProjectId(A_BUILDER_PROJECT_ID).withWorkspaceId(A_WORKSPACE_ID);
    when(connectorBuilderService.getConnectorBuilderProject(any(UUID.class), any(boolean.class))).thenReturn(project);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .workspaceId(A_WORKSPACE_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(connectorBuilderService, times(1)).deleteBuilderProjectDraft(A_BUILDER_PROJECT_ID);
  }

  @Test
  void testUpdateTestingValuesOnProjectWithNoExistingValues()
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final JsonNode spec = Jsons.deserialize(
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
        }""");

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);
    when(secretsRepositoryWriter.createFromConfig(workspaceId, testingValues, spec, null))
        .thenReturn(testingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, spec)).thenReturn(testingValuesWithObfuscatedSecrets);

    final JsonNode response = connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        new ConnectorBuilderProjectTestingValuesUpdate().workspaceId(workspaceId).builderProjectId(project.getBuilderProjectId())
            .testingValues(testingValues).spec(spec));
    assertEquals(response, testingValuesWithObfuscatedSecrets);
    verify(connectorBuilderService, times(1)).updateBuilderProjectTestingValues(project.getBuilderProjectId(), testingValuesWithSecretCoordinates);
  }

  @Test
  void testUpdateTestingValuesOnProjectWithExistingValues()
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates);
    final JsonNode newTestingValues = Jsons.deserialize(
        """
        {
          "username": "bob",
          "password": "hunter3"
        }""");
    final JsonNode newTestingValuesWithSecretCoordinates = Jsons.deserialize(
        """
        {
          "username": "bob",
          "password": {
            "_secret": "airbyte_workspace_123_secret_456_v2"
          }
        }""");
    final JsonNode spec = Jsons.deserialize(specString);

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates)).thenReturn(testingValues);
    when(secretsProcessor.copySecrets(testingValues, newTestingValues, spec)).thenReturn(newTestingValues);
    when(secretsRepositoryWriter.updateFromConfig(workspaceId, testingValuesWithSecretCoordinates,
        newTestingValues, spec, null)).thenReturn(newTestingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec)).thenReturn(testingValuesWithObfuscatedSecrets);

    final JsonNode response = connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        new ConnectorBuilderProjectTestingValuesUpdate().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId)
            .testingValues(newTestingValues).spec(spec));
    assertEquals(response, testingValuesWithObfuscatedSecrets);
    verify(connectorBuilderService, times(1)).updateBuilderProjectTestingValues(project.getBuilderProjectId(), newTestingValuesWithSecretCoordinates);
  }

  @Test
  void testReadStreamWithNoExistingTestingValues() throws Exception {
    final ConnectorBuilderProject project = generateBuilderProject();

    testStreamReadForProject(project, Jsons.emptyObject());
  }

  @Test
  void testReadStreamWithExistingTestingValues() throws Exception {
    final ConnectorBuilderProject project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates)).thenReturn(testingValues);

    testStreamReadForProject(project, testingValues);
  }

  private void testStreamReadForProject(final ConnectorBuilderProject project, final JsonNode testingValues) throws Exception {
    final String streamName = "stream1";
    final ConnectorBuilderProjectStreamReadRequestBody projectStreamReadRequestBody = new ConnectorBuilderProjectStreamReadRequestBody()
        .builderProjectId(project.getBuilderProjectId())
        .manifest(project.getManifestDraft())
        .streamName(streamName)
        .workspaceId(project.getWorkspaceId())
        .formGeneratedManifest(false)
        .recordLimit(null)
        .pageLimit(null)
        .sliceLimit(null);

    final StreamReadRequestBody streamReadRequestBody = new StreamReadRequestBody(testingValues, project.getManifestDraft(), streamName, false,
        project.getBuilderProjectId().toString(), null, null, null, List.of(), project.getWorkspaceId().toString());

    final JsonNode record1 = Jsons.deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": 1,
            "name": "Bob"
          }
        }""");
    final JsonNode record2 = Jsons.deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": 2,
            "name": "Alice"
          }
        }""");
    final String responseBody = "[" + Jsons.serialize(record1) + "," + Jsons.serialize(record2) + "]";
    final String requestUrl = "https://api.com/users";
    final int responseStatus = 200;
    final HttpRequest httpRequest = new HttpRequest(requestUrl, HttpMethod.GET, null, null);
    final HttpResponse httpResponse = new HttpResponse(responseStatus, responseBody, null);
    final StreamRead streamRead = new StreamRead(Collections.emptyList(), List.of(
        new StreamReadSlicesInner(List.of(new StreamReadSlicesInnerPagesInner(List.of(record1, record2), httpRequest, httpResponse)), null, null)),
        false, null, null, null, null);

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);
    when(connectorBuilderServerApiClient.readStream(streamReadRequestBody)).thenReturn(streamRead);

    final ConnectorBuilderProjectStreamRead expectedProjectStreamRead = new ConnectorBuilderProjectStreamRead()
        .logs(Collections.emptyList())
        .slices(List.of(new ConnectorBuilderProjectStreamReadSlicesInner()
            .pages(List.of(new ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                .records(List.of(record1, record2))
                .request(new ConnectorBuilderHttpRequest().url(requestUrl).httpMethod(HttpMethodEnum.GET))
                .response(new ConnectorBuilderHttpResponse().status(responseStatus).body(responseBody))))))
        .testReadLimitReached(false);
    final ConnectorBuilderProjectStreamRead actualProjectStreamRead =
        connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody);
    assertEquals(expectedProjectStreamRead, actualProjectStreamRead);
  }

  @Test
  void testReadStreamUpdatesPersistedTestingValues() throws Exception {
    final JsonNode spec = Jsons.deserialize(specString);
    final ConnectorBuilderProject project = generateBuilderProject().withTestingValues(testingValuesWithSecretCoordinates);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates)).thenReturn(testingValues);

    final String streamName = "stream1";
    final ConnectorBuilderProjectStreamReadRequestBody projectStreamReadRequestBody = new ConnectorBuilderProjectStreamReadRequestBody()
        .builderProjectId(project.getBuilderProjectId())
        .manifest(project.getManifestDraft())
        .streamName(streamName)
        .state(List.of())
        .workspaceId(project.getWorkspaceId())
        .formGeneratedManifest(false)
        .recordLimit(null)
        .pageLimit(null)
        .sliceLimit(null);

    final StreamReadRequestBody streamReadRequestBody = new StreamReadRequestBody(testingValues, project.getManifestDraft(), streamName, false,
        project.getBuilderProjectId().toString(), null, null, null, List.of(), project.getWorkspaceId().toString());

    final JsonNode newTestingValues = Jsons.deserialize(
        """
        {
          "username": "alice",
          "password": "hunter3"
        }""");
    final JsonNode newTestingValuesWithSecretCoordinates = Jsons.deserialize(
        """
        {
          "username": "alice",
          "password": {
            "_secret": "airbyte_workspace_123_secret_456_v2"
          }
        }""");
    final JsonNode newTestingValuesWithObfuscatedSecrets = Jsons.deserialize(
        """
        {
          "username": "alice",
          "password": "**********"
        }""");
    final StreamRead streamRead = new StreamRead(Collections.emptyList(), Collections.emptyList(), false, null, null, null, newTestingValues);

    when(connectorBuilderService.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);
    when(connectorBuilderServerApiClient.readStream(streamReadRequestBody)).thenReturn(streamRead);
    when(secretsRepositoryWriter.updateFromConfig(workspaceId, testingValuesWithSecretCoordinates, newTestingValues, spec, null))
        .thenReturn(newTestingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec)).thenReturn(newTestingValuesWithObfuscatedSecrets);

    final ConnectorBuilderProjectStreamRead projectStreamRead =
        connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody);

    assertEquals(newTestingValuesWithObfuscatedSecrets, projectStreamRead.getLatestConfigUpdate());
    verify(connectorBuilderService, times(1)).updateBuilderProjectTestingValues(project.getBuilderProjectId(), newTestingValuesWithSecretCoordinates);
  }

  @Test
  void testGetBaseImageForDeclarativeManifest() {
    final DeclarativeManifestRequestBody requestBody = new DeclarativeManifestRequestBody().manifest(A_MANIFEST);

    when(manifestInjector.getCdkVersion(any())).thenReturn(A_CDK_VERSION);
    when(declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(anyInt()))
        .thenReturn(A_DECLARATIVE_MANIFEST_IMAGE_VERSION);

    final DeclarativeManifestBaseImageRead responseBody = connectorBuilderProjectsHandler.getDeclarativeManifestBaseImage(requestBody);
    assertEquals(A_BASE_IMAGE, responseBody.getBaseImage());
  }

  @Test
  void testGetConnectorBuilderProjectIdBySourceDefinitionId() throws IOException {
    final UUID actorDefinitionId = UUID.randomUUID();
    final UUID projectId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId)).thenReturn(Optional.of(projectId));

    final BuilderProjectForDefinitionResponse response = connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        new BuilderProjectForDefinitionRequestBody().actorDefinitionId(actorDefinitionId).workspaceId(workspaceId));

    assertEquals(projectId, response.getBuilderProjectId());
  }

  @Test
  void testGetConnectorBuilderProjectIdBySourceDefinitionIdWhenNotFound() throws IOException {
    final UUID actorDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    when(connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId)).thenReturn(Optional.empty());

    final BuilderProjectForDefinitionResponse response = connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        new BuilderProjectForDefinitionRequestBody().actorDefinitionId(actorDefinitionId).workspaceId(workspaceId));

    assertNull(response.getBuilderProjectId());
  }

  private static ConnectorBuilderPublishRequestBody anyConnectorBuilderProjectRequest() {
    return new ConnectorBuilderPublishRequestBody().initialDeclarativeManifest(anyInitialManifest());
  }

  private static DeclarativeSourceManifest anyInitialManifest() {
    return new DeclarativeSourceManifest().version(A_VERSION);
  }

  private void setupConnectorSpecificationAdapter(final JsonNode spec, final String documentationUrl) {
    when(manifestInjector.createDeclarativeManifestConnectorSpecification(spec)).thenReturn(adaptedConnectorSpecification);
    when(adaptedConnectorSpecification.getDocumentationUrl()).thenReturn(URI.create(documentationUrl));
  }

  private JsonNode addSpec(final JsonNode manifest) {
    final JsonNode spec = Jsons.deserialize("{\"" + CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}");
    return ((ObjectNode) Jsons.clone(manifest)).set(SPEC_FIELD, spec);
  }

  @Test
  void testCreateForkedConnectorBuilderProjectActorDefinitionIdNotFound() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID baseActorDefinitionId = UUID.randomUUID();
    final ConnectorBuilderProjectForkRequestBody requestBody =
        new ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId);
    when(sourceService.getStandardSourceDefinition(baseActorDefinitionId))
        .thenThrow(new ConfigNotFoundException(ConfigSchema.STANDARD_SOURCE_DEFINITION, baseActorDefinitionId));

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody));
  }

  @Test
  void testCreateForkedConnectorBuilderProjectManifestNotFound() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID baseActorDefinitionId = UUID.randomUUID();
    final ConnectorBuilderProjectForkRequestBody requestBody =
        new ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId);

    final UUID defaultVersionId = UUID.randomUUID();
    final String dockerRepository = "airbyte/source-test";
    final String dockerImageTag = "1.2.3";
    final ActorDefinitionVersion defaultADV = new ActorDefinitionVersion().withActorDefinitionId(baseActorDefinitionId)
        .withDockerRepository(dockerRepository).withDockerImageTag(dockerImageTag);
    when(sourceService.getStandardSourceDefinition(baseActorDefinitionId))
        .thenReturn(new StandardSourceDefinition().withSourceDefinitionId(baseActorDefinitionId).withDefaultVersionId(defaultVersionId));
    when(actorDefinitionService.getActorDefinitionVersion(defaultVersionId)).thenReturn(defaultADV);
    when(remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag)).thenReturn(Optional.empty());

    assertThrows(NotFoundException.class, () -> connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody));
  }

  @Test
  void testCreateForkedConnectorBuilderProject() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    final UUID baseActorDefinitionId = UUID.randomUUID();
    final ConnectorBuilderProjectForkRequestBody requestBody =
        new ConnectorBuilderProjectForkRequestBody().workspaceId(workspaceId).baseActorDefinitionId(baseActorDefinitionId);

    final String connectorName = "Test Connector";
    final UUID baseActorDefinitionVersionId = UUID.randomUUID();
    final String dockerRepository = "airbyte/source-test";
    final String dockerImageTag = "1.2.3";
    final ActorDefinitionVersion defaultADV = new ActorDefinitionVersion().withVersionId(baseActorDefinitionVersionId)
        .withActorDefinitionId(baseActorDefinitionId).withDockerRepository(dockerRepository).withDockerImageTag(dockerImageTag);
    final UUID connectorBuilderProjectId = UUID.randomUUID();

    when(sourceService.getStandardSourceDefinition(baseActorDefinitionId)).thenReturn(
        new StandardSourceDefinition().withSourceDefinitionId(baseActorDefinitionId).withDefaultVersionId(baseActorDefinitionVersionId)
            .withName(connectorName));
    when(actorDefinitionService.getActorDefinitionVersion(baseActorDefinitionVersionId)).thenReturn(defaultADV);
    when(remoteDefinitionsProvider.getConnectorManifest(dockerRepository, dockerImageTag)).thenReturn(Optional.of(draftManifest));
    when(uuidSupplier.get()).thenReturn(connectorBuilderProjectId);

    connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(requestBody);

    verify(connectorBuilderService, times(1))
        .writeBuilderProjectDraft(eq(connectorBuilderProjectId), eq(workspaceId), eq(connectorName), eq(draftManifest),
            eq(baseActorDefinitionVersionId), eq(null), eq(null));
  }

  @Test
  void testGetConnectorBuilderProjectOAuthConsent() throws Exception {
    final UUID projectId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final String redirectUrl = "https://airbyte.com/auth_flow";
    final String consentUrl = "https://consent.url";

    final OAuthConfigSpecification oAuthConfigSpecification = mock(OAuthConfigSpecification.class);
    final ConnectorSpecification spec =
        new ConnectorSpecification().withAdvancedAuth(new AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification));
    final ConnectorBuilderProject project =
        new ConnectorBuilderProject().withManifestDraft(Jsons.jsonNode(Map.of("spec", spec))).withTestingValues(testingValuesWithSecretCoordinates);
    when(connectorBuilderService.getConnectorBuilderProject(projectId, true)).thenReturn(project);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates)).thenReturn(testingValues);

    final DeclarativeOAuthFlow oAuthFlowImplementation = mock(DeclarativeOAuthFlow.class);
    when(oAuthFlowImplementation.getSourceConsentUrl(eq(workspaceId), eq(null), eq(redirectUrl), eq(testingValues),
        any(OAuthConfigSpecification.class), eq(testingValues)))
            .thenReturn(consentUrl);

    when(oauthImplementationFactory.createDeclarativeOAuthImplementation(any(ConnectorSpecification.class)))
        .thenReturn(oAuthFlowImplementation);

    final BuilderProjectOauthConsentRequest request = new BuilderProjectOauthConsentRequest()
        .builderProjectId(projectId)
        .workspaceId(workspaceId)
        .redirectUrl(redirectUrl);

    final OAuthConsentRead response = connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(request);

    verify(oAuthFlowImplementation, times(1)).getSourceConsentUrl(eq(workspaceId), eq(null), eq(redirectUrl), eq(testingValues),
        any(OAuthConfigSpecification.class), eq(testingValues));
    assertEquals(consentUrl, response.getConsentUrl());
  }

  @Test
  void testCompleteConnectorBuilderProjectOAuth() throws Exception {
    final UUID projectId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final String redirectUrl = "https://airbyte.com/auth_flow";
    final Map<String, Object> queryParams = Map.of("code", "12345");
    final Map<String, Object> oAuthResponse = Map.of("accessToken", "token");

    final OAuthConfigSpecification oAuthConfigSpecification = mock(OAuthConfigSpecification.class);
    final ConnectorSpecification spec =
        new ConnectorSpecification().withAdvancedAuth(new AdvancedAuth().withOauthConfigSpecification(oAuthConfigSpecification));
    final ConnectorBuilderProject project =
        new ConnectorBuilderProject().withManifestDraft(Jsons.jsonNode(Map.of("spec", spec))).withTestingValues(testingValuesWithSecretCoordinates);
    when(connectorBuilderService.getConnectorBuilderProject(projectId, true)).thenReturn(project);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(testingValuesWithSecretCoordinates)).thenReturn(testingValues);

    final DeclarativeOAuthFlow oAuthFlowMock = mock(DeclarativeOAuthFlow.class);
    when(oAuthFlowMock.completeSourceOAuth(eq(workspaceId), eq(null), eq(queryParams), eq(redirectUrl), eq(testingValues),
        any(OAuthConfigSpecification.class), eq(testingValues)))
            .thenReturn(oAuthResponse);

    when(oauthImplementationFactory.createDeclarativeOAuthImplementation(any(ConnectorSpecification.class)))
        .thenReturn(oAuthFlowMock);

    when(connectorBuilderService.getConnectorBuilderProject(eq(projectId), eq(true))).thenReturn(project);

    final CompleteConnectorBuilderProjectOauthRequest request = new CompleteConnectorBuilderProjectOauthRequest()
        .builderProjectId(projectId)
        .workspaceId(workspaceId)
        .queryParams(queryParams)
        .redirectUrl(redirectUrl);

    final CompleteOAuthResponse response = connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(request);
    final CompleteOAuthResponse expectedResponse = new CompleteOAuthResponse().requestSucceeded(true).authPayload(oAuthResponse);
    verify(oAuthFlowMock, times(1)).completeSourceOAuth(eq(workspaceId), eq(null), eq(queryParams), eq(redirectUrl), eq(testingValues),
        any(OAuthConfigSpecification.class), eq(testingValues));
    assertEquals(expectedResponse, response);
  }

}
