/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.protocol.models.CatalogHelpers.createAirbyteStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.commons.server.helpers.SourceHelpers;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsRepositoryReader;
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

  private ConfigRepository configRepository;
  private SecretsRepositoryReader secretsRepositoryReader;
  private StandardSourceDefinition standardSourceDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
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
  private TestClient featureFlagClient;

  private static final String SHOES = "shoes";
  private static final String SKU = "sku";
  private static final AirbyteCatalog airbyteCatalog = CatalogHelpers.createAirbyteCatalog(SHOES,
      Field.of(SKU, JsonSchemaType.STRING));

  // needs to match name of file in src/test/resources/icons
  private static final String ICON = "test-source.svg";
  private SourceService sourceService;
  private WorkspaceService workspaceService;
  private SecretPersistenceConfigService secretPersistenceConfigService;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException {
    configRepository = mock(ConfigRepository.class);
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

    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification();

    standardSourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withIcon(ICON);

    sourceDefinitionVersion = new ActorDefinitionVersion()
        .withDockerRepository("thebestrepo")
        .withDocumentationUrl("https://wikipedia.org")
        .withDockerImageTag("thelatesttag")
        .withSpec(connectorSpecification);

    sourceDefinitionSpecificationRead = new SourceDefinitionSpecificationRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString());

    sourceConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());

    sourceHandler = new SourceHandler(configRepository,
        secretsRepositoryReader,
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper, featureFlagClient, sourceService, workspaceService, secretPersistenceConfigService);
  }

  @Test
  void testCreateSource() throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceCreate sourceCreate = new SourceCreate()
        .name(sourceConnection.getName())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration());

    when(uuidGenerator.get()).thenReturn(sourceConnection.getSourceId());
    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(),
        sourceCreate.getConnectionConfiguration(), sourceDefinitionVersion.getSpec())).thenReturn(sourceCreate.getConnectionConfiguration());
    when(secretsProcessor.prepareSecretsForOutput(sourceCreate.getConnectionConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceCreate.getConnectionConfiguration());

    final SourceRead actualSourceRead = sourceHandler.createSource(sourceCreate);

    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition)
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
  void testUpdateSource() throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
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
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId()))
        .thenReturn(standardSourceDefinition);
    when(configRepository.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(configurationUpdate.source(sourceConnection.getSourceId(), updatedSourceName, newConfiguration))
        .thenReturn(expectedSourceConnection);

    final SourceRead actualSourceRead = sourceHandler.updateSource(sourceUpdate);
    final SourceRead expectedSourceRead =
        SourceHelpers.getSourceRead(expectedSourceConnection, standardSourceDefinition).connectionConfiguration(newConfiguration);

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

    final UUID newDefaultVersionId = UUID.randomUUID();
    final StandardSourceDefinition sourceDefinitionWithNewVersion = Jsons.clone(standardSourceDefinition)
        .withDefaultVersionId(newDefaultVersionId);

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionWithNewVersion.getSourceDefinitionId()))
        .thenReturn(sourceDefinitionWithNewVersion);

    sourceHandler.upgradeSourceVersion(sourceIdRequestBody);

    // validate that we set the actor version to the actor definition (global) default version
    verify(configRepository).setActorDefaultVersion(sourceConnection.getSourceId(), newDefaultVersionId);
  }

  @Test
  void testGetSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(expectedSourceRead.getSourceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    final SourceRead actualSourceRead = sourceHandler.getSource(sourceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceRead);

    // make sure the icon was loaded into actual svg content
    assertTrue(expectedSourceRead.getIcon().startsWith("<svg>"));

    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId());
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testCloneSourceWithoutConfigChange()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceConnection clonedConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());
    final SourceRead expectedClonedSourceRead = SourceHelpers.getSourceRead(clonedConnection, standardSourceDefinition);
    final SourceRead sourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);

    final SourceCloneRequestBody sourceCloneRequestBody = new SourceCloneRequestBody().sourceCloneId(sourceRead.getSourceId());

    when(uuidGenerator.get()).thenReturn(clonedConnection.getSourceId());
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getSourceConnection(clonedConnection.getSourceId())).thenReturn(clonedConnection);

    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    final SourceRead actualSourceRead = sourceHandler.cloneSource(sourceCloneRequestBody);

    assertEquals(expectedClonedSourceRead, actualSourceRead);
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId());
  }

  @Test
  void testCloneSourceWithConfigChange()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceConnection clonedConnection = SourceHelpers.generateSource(standardSourceDefinition.getSourceDefinitionId());
    final SourceRead expectedClonedSourceRead = SourceHelpers.getSourceRead(clonedConnection, standardSourceDefinition);
    final SourceRead sourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);

    final SourceCloneConfiguration sourceCloneConfiguration = new SourceCloneConfiguration().name("Copy Name");
    final SourceCloneRequestBody sourceCloneRequestBody =
        new SourceCloneRequestBody().sourceCloneId(sourceRead.getSourceId()).sourceConfiguration(sourceCloneConfiguration);

    when(uuidGenerator.get()).thenReturn(clonedConnection.getSourceId());
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getSourceConnection(clonedConnection.getSourceId())).thenReturn(clonedConnection);

    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    final SourceRead actualSourceRead = sourceHandler.cloneSource(sourceCloneRequestBody);

    assertEquals(expectedClonedSourceRead, actualSourceRead);
  }

  @Test
  void testListSourcesForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);

    when(configRepository.listWorkspaceSourceConnection(sourceConnection.getWorkspaceId())).thenReturn(Lists.newArrayList(sourceConnection));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    final SourceReadList actualSourceReadList = sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
    verify(actorDefinitionVersionHelper).getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(),
        sourceConnection.getSourceId());
  }

  @Test
  void testListSourcesForSourceDefinition() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);
    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceConnection.getSourceDefinitionId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.listSourcesForDefinition(sourceConnection.getSourceDefinitionId())).thenReturn(Lists.newArrayList(sourceConnection));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    final SourceReadList actualSourceReadList = sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody);

    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));
    verify(secretsProcessor).prepareSecretsForOutput(sourceConnection.getConfiguration(),
        sourceDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testSearchSources() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceRead expectedSourceRead = SourceHelpers.getSourceRead(sourceConnection, standardSourceDefinition);

    when(configRepository.getSourceConnection(sourceConnection.getSourceId())).thenReturn(sourceConnection);
    when(configRepository.listSourceConnection()).thenReturn(Lists.newArrayList(sourceConnection));
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    when(connectionsHandler.matchSearch(new SourceSearch(), expectedSourceRead)).thenReturn(true);
    SourceReadList actualSourceReadList = sourceHandler.searchSources(new SourceSearch());
    assertEquals(1, actualSourceReadList.getSources().size());
    assertEquals(expectedSourceRead, actualSourceReadList.getSources().get(0));

    when(connectionsHandler.matchSearch(new SourceSearch(), expectedSourceRead)).thenReturn(false);
    actualSourceReadList = sourceHandler.searchSources(new SourceSearch());
    assertEquals(0, actualSourceReadList.getSources().size());
  }

  @Test
  void testDeleteSource() throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final JsonNode newConfiguration = sourceConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put("apiKey", "987-xyz");

    final SourceConnection expectedSourceConnection = Jsons.clone(sourceConnection).withTombstone(true);

    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceConnection.getSourceId());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(sourceConnection.getSourceId());
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(Collections.singletonList(connectionRead));
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(sourceConnection.getWorkspaceId());

    when(configRepository.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(sourceService.getSourceConnectionWithSecrets(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection)
        .thenReturn(expectedSourceConnection);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionSpecificationRead.getSourceDefinitionId(),
        sourceConnection.getWorkspaceId(),
        newConfiguration, sourceDefinitionVersion.getSpec())).thenReturn(newConfiguration);
    when(configRepository.getStandardSourceDefinition(sourceDefinitionSpecificationRead.getSourceDefinitionId()))
        .thenReturn(standardSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId()))
        .thenReturn(sourceDefinitionVersion);
    when(configRepository.getSourceDefinitionFromSource(sourceConnection.getSourceId())).thenReturn(standardSourceDefinition);
    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)).thenReturn(connectionReadList);
    when(
        secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), sourceDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(sourceConnection.getConfiguration());

    sourceHandler.deleteSource(sourceIdRequestBody);

    verify(sourceService).writeSourceConnectionWithSecrets(expectedSourceConnection, connectorSpecification);
    verify(connectionsHandler).listConnectionsForWorkspace(workspaceIdRequestBody);
    verify(connectionsHandler).deleteConnection(connectionRead.getConnectionId());
  }

  @Test
  void testWriteDiscoverCatalogResult() throws JsonValidationException, IOException {
    final UUID actorId = UUID.randomUUID();
    final UUID catalogId = UUID.randomUUID();
    final String connectorVersion = "0.0.1";
    final String hashValue = "0123456789abcd";

    final SourceDiscoverSchemaWriteRequestBody request = new SourceDiscoverSchemaWriteRequestBody()
        .catalog(CatalogConverter.toApi(airbyteCatalog, new ActorDefinitionVersion()))
        .sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue);

    when(configRepository.writeActorCatalogFetchEvent(airbyteCatalog, actorId, connectorVersion, hashValue)).thenReturn(catalogId);
    final DiscoverCatalogResult result = sourceHandler.writeDiscoverCatalogResult(request);

    verify(configRepository).writeActorCatalogFetchEvent(airbyteCatalog, actorId, connectorVersion, hashValue);
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
        CatalogConverter.toApi(airbyteCatalogWithOneStream, advNoSuggestedStreams)).sourceId(actorId).connectorVersion(connectorVersion)
        .configurationHash(hashValue);
    final SourceDiscoverSchemaWriteRequestBody requestTwo = new SourceDiscoverSchemaWriteRequestBody().catalog(
        CatalogConverter.toApi(airbyteCatalogWithTwoUnsuggestedStreams, advNoSuggestedStreams)).sourceId(actorId)
        .connectorVersion(connectorVersion)
        .configurationHash(hashValue);
    final SourceDiscoverSchemaWriteRequestBody requestThree = new SourceDiscoverSchemaWriteRequestBody().catalog(
        CatalogConverter.toApi(airbyteCatalogWithOneSuggestedAndOneUnsuggestedStream, advOneSuggestedStream)).sourceId(actorId)
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
  void testCreateSourceHandleSecret() throws JsonValidationException, ConfigNotFoundException, IOException {
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
    when(configRepository.getStandardSourceDefinition(sourceCreate.getSourceDefinitionId()))
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
