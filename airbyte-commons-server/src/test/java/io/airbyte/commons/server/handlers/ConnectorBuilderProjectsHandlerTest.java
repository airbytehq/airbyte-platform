/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.CONNECTION_SPECIFICATION_FIELD;
import static io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler.SPEC_FIELD;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest;
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest.HttpMethodEnum;
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse;
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails;
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
import io.airbyte.api.model.generated.DeclarativeSourceManifest;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.init.CdkVersionProvider;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest.HttpMethod;
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner;
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectorBuilderProjectsHandlerTest {

  private static final UUID A_SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID A_BUILDER_PROJECT_ID = UUID.randomUUID();
  private static final UUID A_WORKSPACE_ID = UUID.randomUUID();
  private static final Long A_VERSION = 32L;
  private static final Long ACTIVE_MANIFEST_VERSION = 865L;
  private static final String A_DESCRIPTION = "a description";
  private static final String A_SOURCE_NAME = "a source name";
  private static final String A_NAME = "a name";
  private static final String A_DOCUMENTATION_URL = "http://documentation.url";
  private static final JsonNode A_MANIFEST;
  private static final JsonNode A_SPEC;
  private static final ActorDefinitionConfigInjection A_CONFIG_INJECTION = new ActorDefinitionConfigInjection().withInjectionPath("something");
  private static final String CDK_VERSION = "8.9.10";

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigRepository configRepository;
  private BuilderProjectUpdater builderProjectUpdater;
  private ConnectorBuilderProjectsHandler connectorBuilderProjectsHandler;
  private Supplier<UUID> uuidSupplier;
  private DeclarativeSourceManifestInjector manifestInjector;
  private CdkVersionProvider cdkVersionProvider;
  private WorkspaceService workspaceService;
  private FeatureFlagClient featureFlagClient;
  private SecretsRepositoryReader secretsRepositoryReader;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private ConnectorBuilderService connectorBuilderService;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorBuilderServerApi connectorBuilderServerApiClient;
  private ConnectorSpecification adaptedConnectorSpecification;
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
    configRepository = mock(ConfigRepository.class);
    builderProjectUpdater = mock(BuilderProjectUpdater.class);
    uuidSupplier = mock(Supplier.class);
    manifestInjector = mock(DeclarativeSourceManifestInjector.class);
    cdkVersionProvider = mock(CdkVersionProvider.class);
    workspaceService = mock(WorkspaceService.class);
    featureFlagClient = mock(TestClient.class);
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    connectorBuilderService = mock(ConnectorBuilderService.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    connectorBuilderServerApiClient = mock(ConnectorBuilderServerApi.class);
    when(cdkVersionProvider.getCdkVersion()).thenReturn(CDK_VERSION);
    adaptedConnectorSpecification = mock(ConnectorSpecification.class);
    setupConnectorSpecificationAdapter(any(), "");
    workspaceId = UUID.randomUUID();

    connectorBuilderProjectsHandler =
        new ConnectorBuilderProjectsHandler(configRepository, builderProjectUpdater, cdkVersionProvider, uuidSupplier, manifestInjector,
            workspaceService, featureFlagClient,
            secretsRepositoryReader, secretsRepositoryWriter, secretPersistenceConfigService, connectorBuilderService, secretsProcessor,
            connectorBuilderServerApiClient);
  }

  private ConnectorBuilderProject generateBuilderProject() throws JsonProcessingException {
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

    verify(configRepository, times(1))
        .writeBuilderProjectDraft(
            project.getBuilderProjectId(), project.getWorkspaceId(), project.getName(), project.getManifestDraft());
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
    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.updateConnectorBuilderProject(update));

    verify(configRepository, never()).writeBuilderProjectDraft(any(UUID.class), any(UUID.class), any(String.class), any(JsonNode.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should validate whether the workspace does not match")
  void testDeleteConnectorBuilderProjectValidateWorkspace() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    final UUID wrongWorkspace = UUID.randomUUID();

    project.setWorkspaceId(wrongWorkspace);
    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    assertThrows(ConfigNotFoundException.class, () -> connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId)));

    verify(configRepository, never()).deleteBuilderProject(any(UUID.class));
  }

  @Test
  @DisplayName("deleteConnectorBuilderProject should delete an existing project")
  void testDeleteConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();

    when(configRepository.getConnectorBuilderProject(project.getBuilderProjectId(), false)).thenReturn(project);

    connectorBuilderProjectsHandler.deleteConnectorBuilderProject(
        new ConnectorBuilderProjectIdWithWorkspaceId().builderProjectId(project.getBuilderProjectId()).workspaceId(workspaceId));

    verify(configRepository, times(1))
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

    when(configRepository.getConnectorBuilderProjectsByWorkspace(workspaceId)).thenReturn(Stream.of(project1, project2));

    final ConnectorBuilderProjectReadList response =
        connectorBuilderProjectsHandler.listConnectorBuilderProjects(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(project1.getBuilderProjectId(), response.getProjects().get(0).getBuilderProjectId());
    assertEquals(project2.getBuilderProjectId(), response.getProjects().get(1).getBuilderProjectId());

    assertTrue(response.getProjects().get(0).getHasDraft());
    assertFalse(response.getProjects().get(1).getHasDraft());

    assertEquals(project1.getActiveDeclarativeManifestVersion(), response.getProjects().get(0).getActiveDeclarativeManifestVersion());
    assertEquals(project1.getActorDefinitionId(), response.getProjects().get(0).getSourceDefinitionId());
    Assertions.assertNull(project2.getActiveDeclarativeManifestVersion());
    Assertions.assertNull(project2.getActorDefinitionId());

    verify(configRepository, times(1))
        .getConnectorBuilderProjectsByWorkspace(
            workspaceId);
  }

  @Test
  @DisplayName("getConnectorBuilderProject should return a builder project with draft and retain object structures without primitive leafs")
  void testGetConnectorBuilderProject() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setActorDefinitionId(UUID.randomUUID());
    project.setActiveDeclarativeManifestVersion(A_VERSION);
    project.setTestingValues(testingValuesWithSecretCoordinates);

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
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
  void testGetConnectorBuilderProjectNullTestingValues() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setActorDefinitionId(UUID.randomUUID());
    project.setActiveDeclarativeManifestVersion(A_VERSION);
    project.setTestingValues(null);

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

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
  void testGetConnectorBuilderProjectWithoutDraft() throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject();
    project.setManifestDraft(null);
    project.setHasDraft(false);
    project.setTestingValues(null);

    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);

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
  @DisplayName("getConnectorBuilderProject should return a builder project even if there is no draft")
  void givenNoVersionButActiveManifestWhenGetConnectorBuilderProjectWithManifestThenReturnActiveVersion()
      throws IOException, ConfigNotFoundException {
    final ConnectorBuilderProject project = generateBuilderProject()
        .withManifestDraft(null)
        .withHasDraft(false)
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withTestingValues(testingValuesWithSecretCoordinates);
    final JsonNode manifest = addSpec(A_MANIFEST);
    when(configRepository.getConnectorBuilderProject(eq(project.getBuilderProjectId()), any(Boolean.class))).thenReturn(project);
    when(configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(A_SOURCE_DEFINITION_ID)).thenReturn(new DeclarativeManifest()
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
  void givenVersionWhenGetConnectorBuilderProjectWithManifestThenReturnSpecificVersion() throws ConfigNotFoundException, IOException {
    final JsonNode manifest = addSpec(A_MANIFEST);
    when(configRepository.getConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(false))).thenReturn(
        new ConnectorBuilderProject().withWorkspaceId(A_WORKSPACE_ID));
    when(configRepository.getVersionedConnectorBuilderProject(eq(A_BUILDER_PROJECT_ID), eq(A_VERSION))).thenReturn(
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
  void whenPublishConnectorBuilderProjectThenReturnActorDefinition() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    final SourceDefinitionIdBody response = connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest());
    assertEquals(A_SOURCE_DEFINITION_ID, response.getSourceDefinitionId());
  }

  @Test
  void whenPublishConnectorBuilderProjectThenCreateActorDefinition() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);
    when(manifestInjector.createConfigInjection(A_SOURCE_DEFINITION_ID, A_MANIFEST)).thenReturn(A_CONFIG_INJECTION);
    setupConnectorSpecificationAdapter(A_SPEC, A_DOCUMENTATION_URL);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().workspaceId(workspaceId).name(A_SOURCE_NAME)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC)));

    verify(manifestInjector, times(1)).addInjectedDeclarativeManifest(A_SPEC);
    verify(configRepository, times(1)).writeCustomConnectorMetadata(eq(new StandardSourceDefinition()
        .withSourceDefinitionId(A_SOURCE_DEFINITION_ID)
        .withName(A_SOURCE_NAME)
        .withSourceType(SourceType.CUSTOM)
        .withTombstone(false)
        .withPublic(false)
        .withCustom(true)), eq(
            new ActorDefinitionVersion()
                .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
                .withDockerRepository("airbyte/source-declarative-manifest")
                .withDockerImageTag(CDK_VERSION)
                .withSpec(adaptedConnectorSpecification)
                .withSupportLevel(SupportLevel.NONE)
                .withReleaseStage(ReleaseStage.CUSTOM)
                .withDocumentationUrl(A_DOCUMENTATION_URL)
                .withProtocolVersion("0.2.0")),
        eq(workspaceId),
        eq(ScopeType.WORKSPACE));
    verify(configRepository, times(1)).writeActorDefinitionConfigInjectionForPath(eq(A_CONFIG_INJECTION));
  }

  @Test
  void whenPublishConnectorBuilderProjectThenUpdateConnectorBuilderProject() throws IOException {
    when(uuidSupplier.get()).thenReturn(A_SOURCE_DEFINITION_ID);

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).insertActiveDeclarativeManifest(eq(new DeclarativeManifest()
        .withActorDefinitionId(A_SOURCE_DEFINITION_ID)
        .withVersion(A_VERSION)
        .withDescription(A_DESCRIPTION)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)));
    verify(configRepository, times(1)).assignActorDefinitionToConnectorBuilderProject(A_BUILDER_PROJECT_ID, A_SOURCE_DEFINITION_ID);
  }

  @Test
  void whenPublishConnectorBuilderProjectThenDraftDeleted() throws IOException {
    connectorBuilderProjectsHandler.publishConnectorBuilderProject(anyConnectorBuilderProjectRequest().builderProjectId(A_BUILDER_PROJECT_ID)
        .initialDeclarativeManifest(anyInitialManifest().manifest(A_MANIFEST).spec(A_SPEC).version(A_VERSION).description(A_DESCRIPTION)));

    verify(configRepository, times(1)).deleteBuilderProjectDraft(A_BUILDER_PROJECT_ID);
  }

  @Test
  void testUpdateTestingValuesOnProjectWithNoExistingValues()
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
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
    when(secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(workspaceId, Optional.empty(), testingValues, spec, true))
        .thenReturn(testingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(testingValuesWithSecretCoordinates, spec)).thenReturn(testingValuesWithObfuscatedSecrets);

    final JsonNode response = connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        new ConnectorBuilderProjectTestingValuesUpdate().builderProjectId(project.getBuilderProjectId()).testingValues(testingValues).spec(spec));
    assertEquals(response, testingValuesWithObfuscatedSecrets);
    verify(connectorBuilderService, times(1)).updateBuilderProjectTestingValues(project.getBuilderProjectId(), testingValuesWithSecretCoordinates);
  }

  @Test
  void testUpdateTestingValuesOnProjectWithExistingValues()
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
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
    when(secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(workspaceId, Optional.of(testingValuesWithSecretCoordinates),
        newTestingValues, spec, true)).thenReturn(newTestingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec)).thenReturn(testingValuesWithObfuscatedSecrets);

    final JsonNode response = connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        new ConnectorBuilderProjectTestingValuesUpdate().builderProjectId(project.getBuilderProjectId()).testingValues(newTestingValues).spec(spec));
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

  private void testStreamReadForProject(ConnectorBuilderProject project, JsonNode testingValues) throws Exception {
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
        project.getBuilderProjectId().toString(), null, null, null, null, project.getWorkspaceId().toString());

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
        .workspaceId(project.getWorkspaceId())
        .formGeneratedManifest(false)
        .recordLimit(null)
        .pageLimit(null)
        .sliceLimit(null);

    final StreamReadRequestBody streamReadRequestBody = new StreamReadRequestBody(testingValues, project.getManifestDraft(), streamName, false,
        project.getBuilderProjectId().toString(), null, null, null, null, project.getWorkspaceId().toString());

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
    when(secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(workspaceId, Optional.of(testingValuesWithSecretCoordinates),
        newTestingValues, spec, true)).thenReturn(newTestingValuesWithSecretCoordinates);
    when(secretsProcessor.prepareSecretsForOutput(newTestingValuesWithSecretCoordinates, spec)).thenReturn(newTestingValuesWithObfuscatedSecrets);

    final ConnectorBuilderProjectStreamRead projectStreamRead =
        connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(projectStreamReadRequestBody);

    assertEquals(newTestingValuesWithObfuscatedSecrets, projectStreamRead.getLatestConfigUpdate());
    verify(connectorBuilderService, times(1)).updateBuilderProjectTestingValues(project.getBuilderProjectId(), newTestingValuesWithSecretCoordinates);
  }

  private static ConnectorBuilderPublishRequestBody anyConnectorBuilderProjectRequest() {
    return new ConnectorBuilderPublishRequestBody().initialDeclarativeManifest(anyInitialManifest());
  }

  private static DeclarativeSourceManifest anyInitialManifest() {
    return new DeclarativeSourceManifest().version(A_VERSION);
  }

  private static ConnectorBuilderProject anyBuilderProject() {
    return new ConnectorBuilderProject();
  }

  private void setupConnectorSpecificationAdapter(final JsonNode spec, final String documentationUrl) {
    when(manifestInjector.createDeclarativeManifestConnectorSpecification(spec)).thenReturn(adaptedConnectorSpecification);
    when(adaptedConnectorSpecification.getDocumentationUrl()).thenReturn(URI.create(documentationUrl));
  }

  private JsonNode addSpec(JsonNode manifest) {
    final JsonNode spec = Jsons.deserialize("{\"" + CONNECTION_SPECIFICATION_FIELD + "\":" + specString + "}");
    return ((ObjectNode) Jsons.clone(manifest)).set(SPEC_FIELD, spec);
  }

}
