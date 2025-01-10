/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.protocol.models.CatalogHelpers.createAirbyteStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.DiscoverCatalogResult;
import io.airbyte.api.model.generated.SourceCloneConfiguration;
import io.airbyte.api.model.generated.SourceCloneRequestBody;
import io.airbyte.api.model.generated.SourceCreate;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.SupportState;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.commons.server.helpers.SourceHelpers;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SourceHandlerTest {

  private SecretsRepositoryReader secretsRepositoryReader;
  private StandardSourceDefinition standardSourceDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private ActorDefinitionVersionWithOverrideStatus sourceDefinitionVersionWithOverrideStatus;
  private SourceDefinitionSpecificationRead sourceDefinitionSpecificationRead;
  private SourceConnection sourceConnection;
  private SourceHandler sourceHandler;
  private JsonSchemaValidator validator;
  private ConnectionsHandler connectionsHandler;
  private ConfigurationUpdate configurationUpdate;
  private Supplier<UUID> uuidGenerator;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorSpecification connectorSpecification;
  private OAuthConfigSupplier oAuthConfigSupplier;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;
  private TestClient featureFlagClient;

  private static final String SHOES = "shoes";
  private static final String SKU = "sku";
  private static final AirbyteCatalog airbyteCatalog = CatalogHelpers.createAirbyteCatalog(SHOES,
      Field.of(SKU, JsonSchemaType.STRING));

  private static final String ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg";
  private static final boolean IS_VERSION_OVERRIDE_APPLIED = true;
  private static final SupportState SUPPORT_STATE = SupportState.SUPPORTED;

  private SourceService sourceService;
  private WorkspaceService workspaceService;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private CatalogService catalogService;
  private final CatalogConverter catalogConverter = new CatalogConverter(new FieldGenerator(), Collections.emptyList());
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(catalogConverter);

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    catalogService = mock(CatalogService.class);
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    validator = mock(JsonSchemaValidator.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    configurationUpdate = mock(ConfigurationUpdate.class);
    uuidGenerator = mock(Supplier.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    featureFlagClient = mock(TestClient.class);
    sourceService = mock(SourceService.class);
    workspaceService = mock(WorkspaceService.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);

    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification();

    standardSourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withIcon(ICON_URL);

    sourceDefinitionVersion = new ActorDefinitionVersion()
        .withDockerRepository("thebestrepo")
        .withDocumentationUrl("https://wikipedia.org")
        .withDockerImageTag("thelatesttag")
        .withSpec(connectorSpecification)
        .withSupportState(ActorDefinitionVersion.SupportState.SUPPORTED);

    sourceDefinitionVersionWithOverrideStatus = new ActorDefinitionVersionWithOverrideStatus(sourceDefinitionVersion, IS_VERSION_OVERRIDE_APPLIED);

    sourceDefinitionSpecificationRead = new SourceDefinitionSpecificationRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString());

    sourceConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());

    sourceHandler = new SourceHandler(
        catalogService,
        secretsRepositoryReader,
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        featureFlagClient,
        sourceService,
        workspaceService,
        secretPersistenceConfigService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater, catalogConverter, apiPojoConverters);
  }

  @Test
  void testCreateSource()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceCreate sourceCreate = new SourceCreate()
        .name(sourceConnection.getName())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration());

    when(uuidGenerator.get()).thenReturn(sourceConnection.getSourceId());
    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(),
        sourceCreate.getConnectionConfiguration(), sourceDefinitionVersion.getSpec())).thenReturn(sourceCreate.getConnectionConfiguration());
    when(secretsProcessor.prepareSecretsForOutput(sourceCreate.getConnectionConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceCreate.getConnectionConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceRead actualSourceRead = sourceHandler.createSource(sourceCreate);

    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE)
            .connectionConfiguration(sourceConnection.getConfiguration());

    assertEquals(expectedSourceRead, actualSourceRead);

    verify(secretsProcessor).prepareSecretsForOutput(sourceCreate.getConnectionConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(oAuthConfigSupplier).maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(), sourceCreate.getConnectionConfiguration(), sourceDefinitionVersion.getSpec());
    verify(sourceService).writeSourceConnectionWithSecrets(sourceConnection, connectorSpecification);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId());
    verify(validator).ensure(sourceDefinitionSpecificationRead.getConnectionSpecification(), sourceConnection.getConfiguration());
  }

  @Test
  void testUpdateSource()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final String updatedSourceName = "my updated source name";
    final JsonNode newConfiguration = sourceConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final SourceConnection expectedSourceConnection = Jsons.clone(sourceConnection)
        .withName(updatedSourceName)
        .withConfiguration(newConfiguration)
        .withTombstone(false);

    final SourceUpdate sourceUpdate = new SourceUpdate()
        .name(updatedSourceName)
        .sourceId(sourceConnection.getSourceId())
        .connectionConfiguration(newConfiguration);

    when(secretsProcessor
        .copySecrets(sourceConnection.getConfiguration(), newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(newConfiguration);
    when(secretsProcessor.prepareSecretsForOutput(newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(newConfiguration);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(),
        newConfiguration, sourceDefinitionVersion.getSpec())).thenReturn(newConfiguration);
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId()))
        .thenReturn(standardSourceDefinition);
    when(sourceService.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(configurationUpdate.source(sourceConnection.getSourceId(), updatedSourceName, newConfiguration))
        .thenReturn(expectedSourceConnection);
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceRead actualSourceRead = sourceHandler.updateSource(sourceUpdate);
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(expectedSourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE)
            .connectionConfiguration(newConfiguration);

    assertEquals(expectedSourceRead, actualSourceRead);

    verify(secretsProcessor).prepareSecretsForOutput(newConfiguration, sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(oAuthConfigSupplier).maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(), newConfiguration, sourceDefinitionVersion.getSpec());
    verify(sourceService).writeSourceConnectionWithSecrets(expectedSourceConnection, connectorSpecification);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId());
    verify(validator).ensure(sourceDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration);
  }

  @Test
  void testUpgradeSourceVersion() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceConnection.getSourceId());

    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getStandardSourceDefinition(standardSourceDefinition.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);

    sourceHandler.upgradeSourceVersion(sourceIdRequestBody);

    // validate that we call the actorDefinitionVersionUpdater to upgrade the version to global default
    verify(actorDefinitionVersionUpdater).upgradeActorVersion(sourceConnection, standardSourceDefinition);
  }

  @Test
  void testGetSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(expectedSourceRead.getSourceId());

    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceRead actualSourceRead = sourceHandler.getSource(sourceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceRead);

    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId());
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testCloneSourceWithoutConfigChange()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceConnection clonedConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());
    final SourceRead expectedClonedSourceRead =
        SourceHelpers.getSourceRead(clonedConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);
    final SourceRead sourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);

    final SourceCloneRequestBody sourceCloneRequestBody = new SourceCloneRequestBody().sourceCloneId(sourceRead.getSourceId());

    when(uuidGenerator.get()).thenReturn(clonedConnection.getSourceId());
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getSourceConnection(clonedConnection.getSourceId())).thenReturn(clonedConnection);

    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, clonedConnection.getWorkspaceId(),
        clonedConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceRead actualSourceRead = sourceHandler.cloneSource(sourceCloneRequestBody);

    assertEquals(expectedClonedSourceRead, actualSourceRead);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId());
  }

  @Test
  void testCloneSourceWithConfigChange()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceConnection clonedConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());
    final SourceRead expectedClonedSourceRead =
        SourceHelpers.getSourceRead(clonedConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);
    final SourceRead sourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);

    final SourceCloneConfiguration sourceCloneConfiguration = new SourceCloneConfiguration().name("Copy Name");
    final SourceCloneRequestBody sourceCloneRequestBody =
        new SourceCloneRequestBody().sourceCloneId(sourceRead.getSourceId()).sourceConfiguration(sourceCloneConfiguration);

    when(uuidGenerator.get()).thenReturn(clonedConnection.getSourceId());
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getSourceConnection(clonedConnection.getSourceId())).thenReturn(clonedConnection);

    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, clonedConnection.getWorkspaceId(),
        clonedConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceRead actualSourceRead = sourceHandler.cloneSource(sourceCloneRequestBody);

    assertEquals(expectedClonedSourceRead, actualSourceRead);
  }

  @Test
  void testListSourcesForWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);
    expectedSourceRead.setStatus(ActorStatus.INACTIVE); // set inactive by default
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);

    when(sourceService.listWorkspaceSourceConnection(sourceConnection.getWorkspaceId())).thenReturn(Lists.newArrayList(sourceConnection));
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceReadList actualSourceReadList = sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId());
  }

  @Test
  void testListSourcesForSourceDefinition()
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);
    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceConnection.getSourceDefinitionId());

    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.listSourcesForDefinition(sourceConnection.getSourceDefinitionId())).thenReturn(Lists.newArrayList(sourceConnection));
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceReadList actualSourceReadList = sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testSearchSources() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition, IS_VERSION_OVERRIDE_APPLIED, SUPPORT_STATE);

    when(sourceService.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(sourceService.listSourceConnection()).thenReturn(Lists.newArrayList(sourceConnection));
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    final SourceSearch validSourceSearch = new SourceSearch().name(sourceConnection.getName());
    SourceReadList actualSourceReadList = sourceHandler.searchSources(validSourceSearch);
    assertEquals(1, actualSourceReadList.getSources().size());
    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));

    final SourceSearch invalidSourceSearch = new SourceSearch().name("invalid");
    actualSourceReadList = sourceHandler.searchSources(invalidSourceSearch);
    assertEquals(0, actualSourceReadList.getSources().size());
  }

  @Test
  void testDeleteSourceAndDeleteSecrets()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final JsonNode newConfiguration = sourceConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final SourceConnection expectedSourceConnection = Jsons.clone(sourceConnection).withTombstone(true);

    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceConnection.getSourceId());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(sourceConnection.getSourceId());
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(Collections.singletonList(connectionRead));
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(sourceService.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(),
        newConfiguration, sourceDefinitionVersion.getSpec())).thenReturn(newConfiguration);
    when(sourceService.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)).thenReturn(connectionReadList);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId())).thenReturn(sourceDefinitionVersionWithOverrideStatus);

    sourceHandler.deleteSource(sourceIdRequestBody);

    // We should not no longer get secrets or write secrets anymore (since we are deleting the source).
    verify(sourceService, times(0)).writeSourceConnectionWithSecrets(expectedSourceConnection, connectorSpecification);
    verify(sourceService, times(0)).getSourceConnectionWithSecrets(any());
    verify(sourceService).tombstoneSource(any(), any(), any(), any());
    verify(connectionsHandler).listConnectionsForWorkspace(workspaceIdRequestBody);
    verify(connectionsHandler).deleteConnection(connectionRead.getConnectionId());
  }

  @Test
  void testWriteDiscoverCatalogResult() throws JsonValidationException, IOException {
    final UUID actorId = UUID.randomUUID();
    final UUID catalogId = UUID.randomUUID();
    final String connectorVersion = "0.0.1";
    final String hashValue = "0123456789abcd";
    final AirbyteCatalog expectedCatalog = Jsons.clone(airbyteCatalog);
    expectedCatalog.getStreams().forEach(s -> s.withSourceDefinedCursor(false));

    final SourceDiscoverSchemaWriteRequestBody request = new SourceDiscoverSchemaWriteRequestBody()
        .catalog(catalogConverter.toApi(expectedCatalog, new ActorDefinitionVersion()))
        .sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue);

    when(catalogService.writeActorCatalogFetchEvent(expectedCatalog, actorId, connectorVersion, hashValue)).thenReturn(catalogId);
    final DiscoverCatalogResult result = sourceHandler.writeDiscoverCatalogResult(request);

    verify(catalogService).writeActorCatalogFetchEvent(expectedCatalog, actorId, connectorVersion, hashValue);
    assert (result.getCatalogId()).equals(catalogId);
  }

  @Test
  @SuppressWarnings("PMD")
  void testCatalogResultSelectedStreams() {
    final UUID actorId = UUID.randomUUID();
    final String connectorVersion = "0.0.1";
    final String hashValue = "0123456789abcd";

    final ActorDefinitionVersion advNoSuggestedStreams = new ActorDefinitionVersion();
    final ActorDefinitionVersion advOneSuggestedStream = new ActorDefinitionVersion().withSuggestedStreams(
        new SuggestedStreams().withStreams(List.of("streamA")));

    final AirbyteCatalog airbyteCatalogWithOneStream = new AirbyteCatalog().withStreams(
        Lists.newArrayList(createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING))));
    final AirbyteCatalog airbyteCatalogWithTwoUnsuggestedStreams = new AirbyteCatalog().withStreams(
        Lists.newArrayList(
            createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING)),
            createAirbyteStream("streamB", Field.of(SKU, JsonSchemaType.STRING))));
    final AirbyteCatalog airbyteCatalogWithOneSuggestedAndOneUnsuggestedStream = new AirbyteCatalog().withStreams(
        Lists.newArrayList(
            createAirbyteStream("streamA", Field.of(SKU, JsonSchemaType.STRING)),
            createAirbyteStream("streamB", Field.of(SKU, JsonSchemaType.STRING))));

    final SourceDiscoverSchemaWriteRequestBody requestOne = new SourceDiscoverSchemaWriteRequestBody().catalog(
        catalogConverter.toApi(airbyteCatalogWithOneStream, advNoSuggestedStreams)).sourceId(actorId).connectorVersion(connectorVersion)
        .configurationHash(hashValue);
    final SourceDiscoverSchemaWriteRequestBody requestTwo = new SourceDiscoverSchemaWriteRequestBody().catalog(
        catalogConverter.toApi(airbyteCatalogWithTwoUnsuggestedStreams, advNoSuggestedStreams)).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue);
    final SourceDiscoverSchemaWriteRequestBody requestThree = new SourceDiscoverSchemaWriteRequestBody().catalog(
        catalogConverter.toApi(airbyteCatalogWithOneSuggestedAndOneUnsuggestedStream, advOneSuggestedStream)).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue);

    assertEquals(1, requestOne.getCatalog().getStreams().size());
    requestOne.getCatalog().getStreams().forEach(s -> assertEquals(true, s.getConfig().getSelected()));
    requestOne.getCatalog().getStreams().forEach(s -> assertEquals(true, s.getConfig().getSuggested()));

    assertEquals(2, requestTwo.getCatalog().getStreams().size());
    requestTwo.getCatalog().getStreams().forEach(s -> assertEquals(false, s.getConfig().getSelected()));
    requestTwo.getCatalog().getStreams().forEach(s -> assertEquals(false, s.getConfig().getSuggested()));

    assertEquals(2, requestThree.getCatalog().getStreams().size());
    final AirbyteStreamConfiguration firstStreamConfig = requestThree.getCatalog().getStreams().get(0).getConfig();
    assertEquals(true, firstStreamConfig.getSuggested());
    assertEquals(true, firstStreamConfig.getSelected());
    final AirbyteStreamConfiguration secondStreamConfig = requestThree.getCatalog().getStreams().get(1).getConfig();
    assertEquals(false, secondStreamConfig.getSuggested());
    assertEquals(false, secondStreamConfig.getSelected());
  }

  @Test
  void testCreateSourceHandleSecret()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final ActorDefinitionVersion oauthDefinitionVersion = new ActorDefinitionVersion()
        .withDockerImageTag("thelatesttag")
        .withSpec(ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification());

    final SourceHandler sourceHandlerSpy = Mockito.spy(sourceHandler);
    final SourceCreate sourceCreate = new SourceCreate()
        .name(sourceConnection.getName())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration());

    doReturn(new SourceRead()).when(sourceHandlerSpy).createSource(any());
    doReturn(Jsons.emptyObject()).when(sourceHandlerSpy).hydrateOAuthResponseSecret(any(), any());
    when(sourceService.getStandardSourceDefinition(sourceCreate.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceCreate.getWorkspaceId()))
        .thenReturn(oauthDefinitionVersion);

    // Test that calling createSourceHandleSecret only hits old code path if nothing is passed for
    // secretId
    sourceHandlerSpy.createSourceWithOptionalSecret(sourceCreate);
    verify(sourceHandlerSpy).createSource(sourceCreate);
    verify(sourceHandlerSpy, never()).hydrateOAuthResponseSecret(any(), any());

    // Test that calling createSourceHandleSecret hits new code path if we have a secretId set.
    final SecretCoordinate secretCoordinate = new SecretCoordinate("test", 1);
    sourceCreate.setSecretId(secretCoordinate.getFullCoordinate());
    sourceHandlerSpy.createSourceWithOptionalSecret(sourceCreate);
    verify(sourceHandlerSpy, times(2)).createSource(sourceCreate);
    verify(sourceHandlerSpy).hydrateOAuthResponseSecret(any(), any());
  }

}
