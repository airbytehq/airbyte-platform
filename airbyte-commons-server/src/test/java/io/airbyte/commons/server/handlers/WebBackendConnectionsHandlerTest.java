/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AttemptRead;
import io.airbyte.api.model.generated.AttemptStatus;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionSchedule;
import io.airbyte.api.model.generated.ConnectionSchedule.TimeUnitEnum;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldAdd;
import io.airbyte.api.model.generated.FieldRemove;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.Geography;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobStatus;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.NamespaceDefinitionType;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.OperationRead;
import io.airbyte.api.model.generated.OperationReadList;
import io.airbyte.api.model.generated.OperationUpdate;
import io.airbyte.api.model.generated.ResourceRequirements;
import io.airbyte.api.model.generated.SchemaChange;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.api.model.generated.WebBackendConnectionCreate;
import io.airbyte.api.model.generated.WebBackendConnectionListItem;
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionRead;
import io.airbyte.api.model.generated.WebBackendConnectionReadList;
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.model.generated.WebBackendOperationCreateOrUpdate;
import io.airbyte.api.model.generated.WebBackendWorkspaceState;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.DestinationHelpers;
import io.airbyte.commons.server.helpers.SourceHelpers;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.temporal.ManualOperationResult;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobStatusSummary;
import io.airbyte.config.RefreshStream.RefreshType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.DestinationAndDefinition;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.CatalogGenerationResult;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class WebBackendConnectionsHandlerTest {

  private ActorDefinitionVersionHandler actorDefinitionVersionHandler;
  private ConnectionsHandler connectionsHandler;
  private OperationsHandler operationsHandler;
  private SchedulerHandler schedulerHandler;
  private StateHandler stateHandler;
  private DestinationHandler destinationHandler;
  private WebBackendConnectionsHandler wbHandler;
  private SourceRead sourceRead;
  private ConnectionRead connectionRead;
  private ConnectionRead brokenConnectionRead;
  private WebBackendConnectionListItem expectedListItem;
  private OperationReadList operationReadList;
  private OperationReadList brokenOperationReadList;
  private WebBackendConnectionRead expected;
  private WebBackendConnectionRead expectedWithNewSchema;
  private WebBackendConnectionRead expectedWithNewSchemaAndBreakingChange;
  private WebBackendConnectionRead expectedWithNewSchemaBroken;
  private WebBackendConnectionRead expectedNoDiscoveryWithNewSchema;
  private EventRunner eventRunner;
  private CatalogService catalogService;
  private ConnectionService connectionService;
  private WorkspaceService workspaceService;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private DestinationCatalogGenerator destinationCatalogGenerator;
  private final FeatureFlagClient featureFlagClient = mock(TestClient.class);
  private final FieldGenerator fieldGenerator = new FieldGenerator();
  private final CatalogConverter catalogConverter = new CatalogConverter(new FieldGenerator(), Collections.emptyList());
  private final ApplySchemaChangeHelper applySchemaChangeHelper = new ApplySchemaChangeHelper(catalogConverter);
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(catalogConverter);

  private static final String STREAM1 = "stream1";
  private static final String STREAM2 = "stream2";
  private static final String FIELD1 = "field1";
  private static final String FIELD2 = "field2";
  private static final String FIELD3 = "field3";
  private static final String FIELD5 = "field5";

  private static final String ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg";

  @BeforeEach
  void setup() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    actorDefinitionVersionHandler = mock(ActorDefinitionVersionHandler.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    stateHandler = mock(StateHandler.class);
    operationsHandler = mock(OperationsHandler.class);
    final JobHistoryHandler jobHistoryHandler = mock(JobHistoryHandler.class);
    catalogService = mock(CatalogService.class);
    connectionService = mock(ConnectionService.class);
    workspaceService = mock(WorkspaceService.class);
    schedulerHandler = mock(SchedulerHandler.class);
    eventRunner = mock(EventRunner.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    destinationCatalogGenerator = mock(DestinationCatalogGenerator.class);

    final JsonSchemaValidator validator = mock(JsonSchemaValidator.class);
    final JsonSecretsProcessor secretsProcessor = mock(JsonSecretsProcessor.class);
    final ConfigurationUpdate configurationUpdate = mock(ConfigurationUpdate.class);
    final OAuthConfigSupplier oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    final DestinationService destinationService = mock(DestinationService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SourceService sourceService = mock(SourceService.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    final Supplier uuidGenerator = mock(Supplier.class);

    destinationHandler = new DestinationHandler(
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        destinationService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        apiPojoConverters);

    final SourceHandler sourceHandler = new SourceHandler(
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
        actorDefinitionVersionUpdater,
        catalogConverter,
        apiPojoConverters);

    wbHandler = spy(new WebBackendConnectionsHandler(
        actorDefinitionVersionHandler,
        connectionsHandler,
        stateHandler,
        sourceHandler,
        destinationHandler,
        jobHistoryHandler,
        schedulerHandler,
        operationsHandler,
        eventRunner,
        catalogService,
        connectionService,
        actorDefinitionVersionHelper,
        fieldGenerator,
        destinationService,
        sourceService,
        workspaceService, catalogConverter,
        applySchemaChangeHelper,
        apiPojoConverters,
        destinationCatalogGenerator));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withIconUrl(ICON_URL);
    final SourceConnection source = SourceHelpers.generateSource(sourceDefinition.getSourceDefinitionId());
    sourceRead = SourceHelpers.getSourceRead(source, sourceDefinition);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
        .withIconUrl(ICON_URL);
    final DestinationConnection destination = DestinationHelpers.generateDestination(destinationDefinition.getDestinationDefinitionId());
    final DestinationRead destinationRead = DestinationHelpers.getDestinationRead(destination, destinationDefinition);

    final StandardSync standardSync =
        ConnectionHelpers.generateSyncWithSourceAndDestinationId(source.getSourceId(), destination.getDestinationId(), false, Status.ACTIVE);
    final StandardSync brokenStandardSync =
        ConnectionHelpers.generateSyncWithSourceAndDestinationId(source.getSourceId(), destination.getDestinationId(), true, Status.INACTIVE);

    when(connectionService.listWorkspaceStandardSyncs(new StandardSyncQuery(sourceRead.getWorkspaceId(), List.of(), List.of(), false)))
        .thenReturn(Collections.singletonList(standardSync));
    when(sourceService.getSourceAndDefinitionsFromSourceIds(Collections.singletonList(source.getSourceId())))
        .thenReturn(Collections.singletonList(new SourceAndDefinition(source, sourceDefinition)));
    when(destinationService.getDestinationAndDefinitionsFromDestinationIds(Collections.singletonList(destination.getDestinationId())))
        .thenReturn(Collections.singletonList(new DestinationAndDefinition(destination, destinationDefinition)));

    when(secretsProcessor.prepareSecretsForOutput(eq(source.getConfiguration()), any())).thenReturn(source.getConfiguration());
    when(secretsProcessor.prepareSecretsForOutput(eq(destination.getConfiguration()), any())).thenReturn(destination.getConfiguration());

    when(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source);
    when(destinationService.getDestinationConnection(destination.getDestinationId())).thenReturn(destination);

    when(sourceService.getSourceDefinitionFromSource(source.getSourceId())).thenReturn(sourceDefinition);
    when(destinationService.getDestinationDefinitionFromDestination(destination.getDestinationId())).thenReturn(destinationDefinition);

    when(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId())).thenReturn(destinationDefinition);

    final ConnectorSpecification mockSpec = mock(ConnectorSpecification.class);
    final ActorDefinitionVersion mockADV = new ActorDefinitionVersion().withSpec(mockSpec);
    final ActorDefinitionVersionWithOverrideStatus mockADVWithOverrideStatus = new ActorDefinitionVersionWithOverrideStatus(mockADV, false);
    when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(any(), any(), any())).thenReturn(mockADVWithOverrideStatus);
    when(actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(any(), any(), any())).thenReturn(mockADVWithOverrideStatus);
    when(actorDefinitionVersionHelper.getSourceVersion(any(), any(), any())).thenReturn(mockADV);
    when(actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any())).thenReturn(mockADV);

    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenAnswer(invocation -> new CatalogGenerationResult(invocation.getArgument(0), Map.of()));

    connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    brokenConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(brokenStandardSync);
    operationReadList = new OperationReadList()
        .operations(List.of(new OperationRead()
            .operationId(connectionRead.getOperationIds().get(0))
            .name("Test Operation")));
    brokenOperationReadList = new OperationReadList()
        .operations(List.of(new OperationRead()
            .operationId(brokenConnectionRead.getOperationIds().get(0))
            .name("Test Operation")));

    final Instant now = Instant.now();
    final JobWithAttemptsRead jobRead = new JobWithAttemptsRead()
        .job(new JobRead()
            .configId(connectionRead.getConnectionId().toString())
            .configType(JobConfigType.SYNC)
            .id(10L)
            .status(JobStatus.SUCCEEDED)
            .createdAt(now.getEpochSecond())
            .updatedAt(now.getEpochSecond()))
        .attempts(Lists.newArrayList(new AttemptRead()
            .id(12L)
            .status(AttemptStatus.SUCCEEDED)
            .bytesSynced(100L)
            .recordsSynced(15L)
            .createdAt(now.getEpochSecond())
            .updatedAt(now.getEpochSecond())
            .endedAt(now.getEpochSecond())));

    when(jobHistoryHandler.getLatestSyncJob(connectionRead.getConnectionId())).thenReturn(Optional.of(jobRead.getJob()));

    when(jobHistoryHandler.getLatestSyncJobsForConnections(Collections.singletonList(connectionRead.getConnectionId())))
        .thenReturn(Collections.singletonList(new JobStatusSummary(UUID.fromString(jobRead.getJob().getConfigId()), jobRead.getJob().getCreatedAt(),
            io.airbyte.config.JobStatus.valueOf(jobRead.getJob().getStatus().toString().toUpperCase()))));

    final JobWithAttemptsRead brokenJobRead = new JobWithAttemptsRead()
        .job(new JobRead()
            .configId(brokenConnectionRead.getConnectionId().toString())
            .configType(JobConfigType.SYNC)
            .id(10L)
            .status(JobStatus.SUCCEEDED)
            .createdAt(now.getEpochSecond())
            .updatedAt(now.getEpochSecond()))
        .attempts(Lists.newArrayList(new AttemptRead()
            .id(12L)
            .status(AttemptStatus.SUCCEEDED)
            .bytesSynced(100L)
            .recordsSynced(15L)
            .createdAt(now.getEpochSecond())
            .updatedAt(now.getEpochSecond())
            .endedAt(now.getEpochSecond())));

    when(jobHistoryHandler.getLatestSyncJob(brokenConnectionRead.getConnectionId())).thenReturn(Optional.of(brokenJobRead.getJob()));

    when(jobHistoryHandler.getLatestSyncJobsForConnections(Collections.singletonList(brokenConnectionRead.getConnectionId())))
        .thenReturn(Collections.singletonList(new JobStatusSummary(UUID.fromString(brokenJobRead.getJob().getConfigId()), brokenJobRead.getJob()
            .getCreatedAt(), io.airbyte.config.JobStatus.valueOf(brokenJobRead.getJob().getStatus().toString().toUpperCase()))));

    expectedListItem = ConnectionHelpers.generateExpectedWebBackendConnectionListItem(
        standardSync,
        sourceRead,
        destinationRead,
        false,
        jobRead.getJob().getCreatedAt(),
        jobRead.getJob().getStatus(),
        SchemaChange.NO_CHANGE);

    expected = expectedWebBackendConnectionReadObject(connectionRead, sourceRead, destinationRead, operationReadList, SchemaChange.NO_CHANGE, now,
        connectionRead.getSyncCatalog(), connectionRead.getSourceCatalogId());
    expectedNoDiscoveryWithNewSchema = expectedWebBackendConnectionReadObject(connectionRead, sourceRead, destinationRead, operationReadList,
        SchemaChange.NON_BREAKING, now, connectionRead.getSyncCatalog(), connectionRead.getSourceCatalogId());

    final AirbyteCatalog modifiedCatalog = ConnectionHelpers.generateMultipleStreamsApiCatalog(2);
    final SourceDiscoverSchemaRequestBody sourceDiscoverSchema = new SourceDiscoverSchemaRequestBody();
    sourceDiscoverSchema.setSourceId(connectionRead.getSourceId());
    sourceDiscoverSchema.setDisableCache(true);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchema)).thenReturn(
        new SourceDiscoverSchemaRead()
            .jobInfo(mock(SynchronousJobRead.class))
            .catalog(modifiedCatalog));

    expectedWithNewSchema = expectedWebBackendConnectionReadObject(connectionRead, sourceRead, destinationRead,
        new OperationReadList().operations(expected.getOperations()), SchemaChange.NON_BREAKING, now, modifiedCatalog, null)
            .catalogDiff(new CatalogDiff().transforms(List.of(
                new StreamTransform().transformType(TransformTypeEnum.ADD_STREAM)
                    .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name("users-data1"))
                    .updateStream(null))));

    expectedWithNewSchemaAndBreakingChange = expectedWebBackendConnectionReadObject(brokenConnectionRead, sourceRead, destinationRead,
        new OperationReadList().operations(expected.getOperations()), SchemaChange.BREAKING, now, modifiedCatalog, null)
            .catalogDiff(new CatalogDiff().transforms(List.of(
                new StreamTransform().transformType(TransformTypeEnum.ADD_STREAM)
                    .streamDescriptor(new io.airbyte.api.model.generated.StreamDescriptor().name("users-data1"))
                    .updateStream(null))));

    expectedWithNewSchemaBroken = expectedWebBackendConnectionReadObject(brokenConnectionRead, sourceRead, destinationRead, brokenOperationReadList,
        SchemaChange.BREAKING, now, connectionRead.getSyncCatalog(), brokenConnectionRead.getSourceCatalogId());
    when(schedulerHandler.resetConnection(any(ConnectionIdRequestBody.class)))
        .thenReturn(new JobInfoRead().job(new JobRead().status(JobStatus.SUCCEEDED)));
  }

  WebBackendConnectionRead expectedWebBackendConnectionReadObject(
                                                                  final ConnectionRead connectionRead,
                                                                  final SourceRead sourceRead,
                                                                  final DestinationRead destinationRead,
                                                                  final OperationReadList operationReadList,
                                                                  final SchemaChange schemaChange,
                                                                  final Instant now,
                                                                  final AirbyteCatalog syncCatalog,
                                                                  final UUID catalogId) {
    return new WebBackendConnectionRead()
        .connectionId(connectionRead.getConnectionId())
        .sourceId(connectionRead.getSourceId())
        .destinationId(connectionRead.getDestinationId())
        .operationIds(connectionRead.getOperationIds())
        .name(connectionRead.getName())
        .namespaceDefinition(connectionRead.getNamespaceDefinition())
        .namespaceFormat(connectionRead.getNamespaceFormat())
        .prefix(connectionRead.getPrefix())
        .syncCatalog(syncCatalog)
        .catalogId(catalogId)
        .status(connectionRead.getStatus())
        .schedule(connectionRead.getSchedule())
        .scheduleType(connectionRead.getScheduleType())
        .scheduleData(connectionRead.getScheduleData())
        .source(sourceRead)
        .destination(destinationRead)
        .operations(operationReadList.getOperations())
        .latestSyncJobCreatedAt(now.getEpochSecond())
        .latestSyncJobStatus(JobStatus.SUCCEEDED)
        .isSyncing(false)
        .schemaChange(schemaChange)
        .resourceRequirements(new ResourceRequirements()
            .cpuRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuRequest())
            .cpuLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuLimit())
            .memoryRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryRequest())
            .memoryLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryLimit()))
        .notifySchemaChanges(false)
        .notifySchemaChangesByEmail(true);
  }

  @Test
  void testGetWorkspaceState() throws IOException {
    final UUID uuid = UUID.randomUUID();
    final WebBackendWorkspaceState request = new WebBackendWorkspaceState().workspaceId(uuid);
    when(workspaceService.countSourcesForWorkspace(uuid)).thenReturn(5);
    when(workspaceService.countDestinationsForWorkspace(uuid)).thenReturn(2);
    when(workspaceService.countConnectionsForWorkspace(uuid)).thenReturn(8);
    final var actual = wbHandler.getWorkspaceState(request);
    assertTrue(actual.getHasConnections());
    assertTrue(actual.getHasDestinations());
    assertTrue((actual.getHasSources()));
  }

  @Test
  void testGetWorkspaceStateEmpty() throws IOException {
    final UUID uuid = UUID.randomUUID();
    final WebBackendWorkspaceState request = new WebBackendWorkspaceState().workspaceId(uuid);
    when(workspaceService.countSourcesForWorkspace(uuid)).thenReturn(0);
    when(workspaceService.countDestinationsForWorkspace(uuid)).thenReturn(0);
    when(workspaceService.countConnectionsForWorkspace(uuid)).thenReturn(0);
    final var actual = wbHandler.getWorkspaceState(request);
    assertFalse(actual.getHasConnections());
    assertFalse(actual.getHasDestinations());
    assertFalse(actual.getHasSources());
  }

  @Test
  void testWebBackendListConnectionsForWorkspace()
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionListRequestBody webBackendConnectionListRequestBody = new WebBackendConnectionListRequestBody();
    webBackendConnectionListRequestBody.setWorkspaceId(sourceRead.getWorkspaceId());

    final WebBackendConnectionReadList WebBackendConnectionReadList =
        wbHandler.webBackendListConnectionsForWorkspace(webBackendConnectionListRequestBody);

    assertEquals(1, WebBackendConnectionReadList.getConnections().size());
    assertEquals(expectedListItem, WebBackendConnectionReadList.getConnections().get(0));

    assertEquals(expectedListItem.getSource().getIcon(), ICON_URL);
    assertEquals(expectedListItem.getDestination().getIcon(), ICON_URL);
  }

  @Test
  void testWebBackendGetConnection()
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody();
    connectionIdRequestBody.setConnectionId(connectionRead.getConnectionId());

    final WebBackendConnectionRequestBody webBackendConnectionRequestBody = new WebBackendConnectionRequestBody();
    webBackendConnectionRequestBody.setConnectionId(connectionRead.getConnectionId());

    when(connectionsHandler.getConnection(connectionRead.getConnectionId())).thenReturn(connectionRead);
    when(operationsHandler.listOperationsForConnection(connectionIdRequestBody)).thenReturn(operationReadList);

    final WebBackendConnectionRead webBackendConnectionRead = wbHandler.webBackendGetConnection(webBackendConnectionRequestBody);

    assertEquals(expected, webBackendConnectionRead);

    assertEquals(expectedListItem.getSource().getIcon(), ICON_URL);
    assertEquals(expectedListItem.getDestination().getIcon(), ICON_URL);
  }

  WebBackendConnectionRead testWebBackendGetConnection(final boolean withCatalogRefresh,
                                                       final ConnectionRead connectionRead,
                                                       final OperationReadList operationReadList)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody();
    connectionIdRequestBody.setConnectionId(connectionRead.getConnectionId());

    final WebBackendConnectionRequestBody webBackendConnectionIdRequestBody = new WebBackendConnectionRequestBody();
    webBackendConnectionIdRequestBody.setConnectionId(connectionRead.getConnectionId());
    if (withCatalogRefresh) {
      webBackendConnectionIdRequestBody.setWithRefreshedCatalog(true);
    }

    when(connectionsHandler.getConnection(connectionRead.getConnectionId())).thenReturn(connectionRead);
    when(operationsHandler.listOperationsForConnection(connectionIdRequestBody)).thenReturn(operationReadList);

    return wbHandler.webBackendGetConnection(webBackendConnectionIdRequestBody);
  }

  @Test
  void testWebBackendGetConnectionWithDiscoveryAndNewSchema() throws ConfigNotFoundException,
      IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID newCatalogId = UUID.randomUUID();
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(newCatalogId)));
    when(catalogService.getActorCatalogById(any())).thenReturn(new ActorCatalog().withId(UUID.randomUUID()));
    final SourceDiscoverSchemaRead schemaRead =
        new SourceDiscoverSchemaRead().catalogDiff(expectedWithNewSchema.getCatalogDiff()).catalog(expectedWithNewSchema.getSyncCatalog())
            .breakingChange(false).connectionStatus(ConnectionStatus.ACTIVE);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(any())).thenReturn(schemaRead);
    when(connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId())).thenReturn(Optional.of(connectionRead.getSyncCatalog()));

    final WebBackendConnectionRead result = testWebBackendGetConnection(true, connectionRead,
        operationReadList);
    assertEquals(expectedWithNewSchema, result);
  }

  @Test
  void testWebBackendGetConnectionWithDiscoveryAndNewSchemaBreakingChange() throws ConfigNotFoundException,
      IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID newCatalogId = UUID.randomUUID();
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(newCatalogId)));
    when(catalogService.getActorCatalogById(any())).thenReturn(new ActorCatalog().withId(UUID.randomUUID()));
    final SourceDiscoverSchemaRead schemaRead =
        new SourceDiscoverSchemaRead().catalogDiff(expectedWithNewSchema.getCatalogDiff()).catalog(expectedWithNewSchema.getSyncCatalog())
            .breakingChange(true).connectionStatus(ConnectionStatus.INACTIVE);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(any())).thenReturn(schemaRead);
    when(connectionsHandler.getConnectionAirbyteCatalog(brokenConnectionRead.getConnectionId()))
        .thenReturn(Optional.of(connectionRead.getSyncCatalog()));

    final WebBackendConnectionRead result = testWebBackendGetConnection(true, brokenConnectionRead,
        operationReadList);
    assertEquals(expectedWithNewSchemaAndBreakingChange, result);
  }

  @Test
  void testWebBackendGetConnectionWithDiscoveryMissingCatalogUsedToMakeConfiguredCatalog()
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID newCatalogId = UUID.randomUUID();
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(newCatalogId)));
    when(catalogService.getActorCatalogById(any())).thenReturn(new ActorCatalog().withId(UUID.randomUUID()));
    final SourceDiscoverSchemaRead schemaRead =
        new SourceDiscoverSchemaRead().catalogDiff(expectedWithNewSchema.getCatalogDiff()).catalog(expectedWithNewSchema.getSyncCatalog())
            .breakingChange(false).connectionStatus(ConnectionStatus.ACTIVE);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(any())).thenReturn(schemaRead);
    when(connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId())).thenReturn(Optional.empty());

    final WebBackendConnectionRead result = testWebBackendGetConnection(true, connectionRead,
        operationReadList);
    assertEquals(expectedWithNewSchema, result);
  }

  @Test
  void testWebBackendGetConnectionWithDiscoveryAndFieldSelectionAddField() throws ConfigNotFoundException,
      IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    // Mock this because the API uses it to determine whether there was a schema change.
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID())));

    // Original configured catalog has two fields, and only one of them is selected.
    final AirbyteCatalog originalConfiguredCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
    originalConfiguredCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true)
        .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(
            ConnectionHelpers.FIELD_NAME)));
    connectionRead.syncCatalog(originalConfiguredCatalog);

    // Original discovered catalog has the same two fields but no selection info because it's a
    // discovered catalog.
    when(connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId())).thenReturn(
        Optional.of(ConnectionHelpers.generateApiCatalogWithTwoFields()));

    // Newly-discovered catalog has an extra field. There is no field selection info because it's a
    // discovered catalog.
    final AirbyteCatalog newCatalogToDiscover = ConnectionHelpers.generateApiCatalogWithTwoFields();
    final JsonNode newFieldSchema = Jsons.deserialize("{\"type\": \"string\"}");
    ((ObjectNode) newCatalogToDiscover.getStreams().get(0).getStream().getJsonSchema().findPath("properties"))
        .putObject("a-new-field")
        .put("type", "string");
    final SourceDiscoverSchemaRead schemaRead =
        new SourceDiscoverSchemaRead()
            .catalogDiff(
                new CatalogDiff()
                    .addTransformsItem(
                        new StreamTransform().updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(new FieldTransform()
                            .transformType(
                                FieldTransform.TransformTypeEnum.ADD_FIELD)
                            .addFieldNameItem("a-new-field").breaking(false)
                            .addField(new FieldAdd().schema(newFieldSchema))))))
            .catalog(newCatalogToDiscover)
            .breakingChange(false)
            .connectionStatus(ConnectionStatus.ACTIVE);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(any())).thenReturn(schemaRead);

    final WebBackendConnectionRead result = testWebBackendGetConnection(true, connectionRead,
        operationReadList);

    // We expect the discovered catalog with two fields selected: the one that was originally selected,
    // plus the newly-discovered field.
    final AirbyteCatalog expectedNewCatalog = Jsons.clone(newCatalogToDiscover);
    expectedNewCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).selectedFields(
        List.of(new SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME), new SelectedFieldInfo().addFieldPathItem("a-new-field")));
    expectedWithNewSchema.catalogDiff(schemaRead.getCatalogDiff()).syncCatalog(expectedNewCatalog);
    assertEquals(expectedWithNewSchema, result);
  }

  @Test
  void testWebBackendGetConnectionWithDiscoveryAndFieldSelectionRemoveField() throws ConfigNotFoundException,
      IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    // Mock this because the API uses it to determine whether there was a schema change.
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID())));

    // Original configured catalog has two fields, and both of them are selected.
    final AirbyteCatalog originalConfiguredCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
    originalConfiguredCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true)
        .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(
            ConnectionHelpers.FIELD_NAME), new SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME + "2")));
    connectionRead.syncCatalog(originalConfiguredCatalog);

    // Original discovered catalog has the same two fields but no selection info because it's a
    // discovered catalog.
    when(connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId())).thenReturn(
        Optional.of(ConnectionHelpers.generateApiCatalogWithTwoFields()));

    // Newly-discovered catalog has one of the fields removed. There is no field selection info because
    // it's a
    // discovered catalog.
    final AirbyteCatalog newCatalogToDiscover = ConnectionHelpers.generateBasicApiCatalog();
    final JsonNode removedFieldSchema = Jsons.deserialize("{\"type\": \"string\"}");
    final SourceDiscoverSchemaRead schemaRead =
        new SourceDiscoverSchemaRead()
            .catalogDiff(
                new CatalogDiff().addTransformsItem(new StreamTransform().updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(
                    new FieldTransform().transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                        .addFieldNameItem(ConnectionHelpers.FIELD_NAME + "2")
                        .breaking(false).removeField(new FieldRemove().schema(removedFieldSchema))))))
            .catalog(newCatalogToDiscover)
            .breakingChange(false)
            .connectionStatus(ConnectionStatus.ACTIVE);
    when(schedulerHandler.discoverSchemaForSourceFromSourceId(any())).thenReturn(schemaRead);

    final WebBackendConnectionRead result = testWebBackendGetConnection(true, connectionRead,
        operationReadList);

    // We expect the discovered catalog with two fields selected: the one that was originally selected,
    // plus the newly-discovered field.
    final AirbyteCatalog expectedNewCatalog = Jsons.clone(newCatalogToDiscover);
    expectedNewCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).selectedFields(
        List.of(new SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME)));
    expectedWithNewSchema.catalogDiff(schemaRead.getCatalogDiff()).syncCatalog(expectedNewCatalog);
    assertEquals(expectedWithNewSchema, result);
  }

  @Test
  void testWebBackendGetConnectionNoRefreshCatalog()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionRead result = testWebBackendGetConnection(false, connectionRead, operationReadList);
    verify(schedulerHandler, never()).discoverSchemaForSourceFromSourceId(any());
    assertEquals(expected, result);
  }

  @Test
  void testWebBackendGetConnectionNoDiscoveryWithNewSchema()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID())));
    when(catalogService.getActorCatalogById(any())).thenReturn(new ActorCatalog().withId(UUID.randomUUID()));
    final WebBackendConnectionRead result = testWebBackendGetConnection(false, connectionRead, operationReadList);
    assertEquals(expectedNoDiscoveryWithNewSchema, result);
  }

  @Test
  void testWebBackendGetConnectionNoDiscoveryWithNewSchemaBreaking()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    when(connectionsHandler.getConnection(brokenConnectionRead.getConnectionId())).thenReturn(brokenConnectionRead);
    when(catalogService.getMostRecentActorCatalogFetchEventForSource(any()))
        .thenReturn(Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID())));
    when(catalogService.getActorCatalogById(any())).thenReturn(new ActorCatalog().withId(UUID.randomUUID()));
    final WebBackendConnectionRead result = testWebBackendGetConnection(false, brokenConnectionRead, brokenOperationReadList);
    assertEquals(expectedWithNewSchemaBroken, result);
  }

  @Test
  void testToConnectionCreate() throws IOException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(source.getSourceId());

    final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
    catalog.getStreams().get(0).getStream().setName("azkaban_users");

    final ConnectionSchedule schedule = new ConnectionSchedule().units(1L).timeUnit(TimeUnitEnum.MINUTES);

    final UUID newSourceId = UUID.randomUUID();
    final UUID newDestinationId = UUID.randomUUID();
    final UUID newOperationId = UUID.randomUUID();
    final UUID sourceCatalogId = UUID.randomUUID();
    final WebBackendConnectionCreate input = new WebBackendConnectionCreate()
        .name("testConnectionCreate")
        .namespaceDefinition(Enums.convertTo(standardSync.getNamespaceDefinition(), NamespaceDefinitionType.class))
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .sourceId(newSourceId)
        .destinationId(newDestinationId)
        .operationIds(List.of(newOperationId))
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .syncCatalog(catalog)
        .sourceCatalogId(sourceCatalogId)
        .geography(Geography.US)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE);

    final List<UUID> operationIds = List.of(newOperationId);

    final ConnectionCreate expected = new ConnectionCreate()
        .name("testConnectionCreate")
        .namespaceDefinition(Enums.convertTo(standardSync.getNamespaceDefinition(), NamespaceDefinitionType.class))
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .sourceId(newSourceId)
        .destinationId(newDestinationId)
        .operationIds(operationIds)
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .syncCatalog(catalog)
        .sourceCatalogId(sourceCatalogId)
        .geography(Geography.US)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE);

    final ConnectionCreate actual = WebBackendConnectionsHandler.toConnectionCreate(input, operationIds);

    assertEquals(expected, actual);
  }

  @Test
  void testToConnectionPatch() throws IOException {
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(source.getSourceId());

    final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
    catalog.getStreams().get(0).getStream().setName("azkaban_users");

    final ConnectionSchedule schedule = new ConnectionSchedule().units(1L).timeUnit(TimeUnitEnum.MINUTES);

    final UUID newOperationId = UUID.randomUUID();
    final WebBackendConnectionUpdate input = new WebBackendConnectionUpdate()
        .namespaceDefinition(Enums.convertTo(standardSync.getNamespaceDefinition(), NamespaceDefinitionType.class))
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .connectionId(standardSync.getConnectionId())
        .operations(List.of(new WebBackendOperationCreateOrUpdate().operationId(newOperationId)))
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .name(standardSync.getName())
        .syncCatalog(catalog)
        .geography(Geography.US)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .notifySchemaChanges(false)
        .notifySchemaChangesByEmail(true);

    final List<UUID> operationIds = List.of(newOperationId);

    final ConnectionUpdate expected = new ConnectionUpdate()
        .namespaceDefinition(Enums.convertTo(standardSync.getNamespaceDefinition(), NamespaceDefinitionType.class))
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .connectionId(standardSync.getConnectionId())
        .operationIds(operationIds)
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .name(standardSync.getName())
        .syncCatalog(catalog)
        .geography(Geography.US)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .notifySchemaChanges(false)
        .notifySchemaChangesByEmail(true)
        .breakingChange(false);

    final ConnectionUpdate actual = WebBackendConnectionsHandler.toConnectionPatch(input, operationIds, false);

    assertEquals(expected, actual);
  }

  @Test
  void testForConnectionCreateCompleteness() {
    final Set<String> handledMethods =
        Set.of("name", "namespaceDefinition", "namespaceFormat", "prefix", "sourceId", "destinationId", "operationIds",
            "addOperationIdsItem", "removeOperationIdsItem", "syncCatalog", "schedule", "scheduleType", "scheduleData",
            "status", "resourceRequirements", "sourceCatalogId", "geography", "nonBreakingChangesPreference", "notifySchemaChanges",
            "notifySchemaChangesByEmail", "backfillPreference");

    final Set<String> methods = Arrays.stream(ConnectionCreate.class.getMethods())
        .filter(method -> method.getReturnType() == ConnectionCreate.class)
        .map(Method::getName)
        .collect(Collectors.toSet());

    final String message =
        """
        If this test is failing, it means you added a field to ConnectionCreate!
        Congratulations, but you're not done yet..
        \tYou should update WebBackendConnectionsHandler::toConnectionCreate
        \tand ensure that the field is tested in WebBackendConnectionsHandlerTest::testToConnectionCreate
        Then you can add the field name here to make this test pass. Cheers!""";
    assertEquals(handledMethods, methods, message);
  }

  @Test
  void testForConnectionPatchCompleteness() {
    final Set<String> handledMethods =
        Set.of("schedule", "connectionId", "syncCatalog", "namespaceDefinition", "namespaceFormat", "prefix", "status",
            "operationIds", "addOperationIdsItem", "removeOperationIdsItem", "resourceRequirements", "name",
            "sourceCatalogId", "scheduleType", "scheduleData", "geography", "breakingChange", "notifySchemaChanges", "notifySchemaChangesByEmail",
            "nonBreakingChangesPreference", "backfillPreference");

    final Set<String> methods = Arrays.stream(ConnectionUpdate.class.getMethods())
        .filter(method -> method.getReturnType() == ConnectionUpdate.class)
        .map(Method::getName)
        .collect(Collectors.toSet());

    final String message =
        """
        If this test is failing, it means you added a field to ConnectionUpdate!
        Congratulations, but you're not done yet..
        \tYou should update WebBackendConnectionsHandler::toConnectionPatch
        \tand ensure that the field is tested in WebBackendConnectionsHandlerTest::testToConnectionPatch
        Then you can add the field name here to make this test pass. Cheers!""";
    assertEquals(handledMethods, methods, message);
  }

  @Test
  void testUpdateConnection()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expected.getSyncCatalog())
        .sourceCatalogId(expected.getCatalogId())
        .notifySchemaChanges(expected.getNotifySchemaChanges())
        .notifySchemaChangesByEmail(expected.getNotifySchemaChangesByEmail());

    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());

    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of());
    when(connectionsHandler.getDiff(any(), any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(expected.getConnectionId());
    when(stateHandler.getState(connectionIdRequestBody)).thenReturn(new ConnectionState().stateType(ConnectionStateType.LEGACY));

    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead().connectionId(expected.getConnectionId()).sourceId(expected.getSourceId()));
    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(
        new ConnectionRead()
            .connectionId(expected.getConnectionId())
            .sourceId(expected.getSourceId())
            .destinationId(expected.getDestinationId())
            .name(expected.getName())
            .namespaceDefinition(expected.getNamespaceDefinition())
            .namespaceFormat(expected.getNamespaceFormat())
            .prefix(expected.getPrefix())
            .syncCatalog(expected.getSyncCatalog())
            .status(expected.getStatus())
            .schedule(expected.getSchedule())
            .breakingChange(false)
            .notifySchemaChanges(expected.getNotifySchemaChanges())
            .notifySchemaChangesByEmail(expected.getNotifySchemaChangesByEmail()));
    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);
    final ConnectionIdRequestBody connectionId = new ConnectionIdRequestBody().connectionId(connectionRead.getConnectionId());

    final AirbyteCatalog fullAirbyteCatalog = ConnectionHelpers.generateMultipleStreamsApiCatalog(2);
    when(connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId())).thenReturn(Optional.ofNullable(fullAirbyteCatalog));

    final AirbyteCatalog expectedCatalogReturned =
        wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(expected.getSyncCatalog(), expected.getSyncCatalog(),
            fullAirbyteCatalog);
    final WebBackendConnectionRead connectionRead = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(expectedCatalogReturned, connectionRead.getSyncCatalog());

    verify(schedulerHandler, times(0)).resetConnection(connectionId);
    verify(schedulerHandler, times(0)).syncConnection(connectionId);
  }

  @Test
  void testUpdateConnectionWithOperations()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendOperationCreateOrUpdate operationCreateOrUpdate = new WebBackendOperationCreateOrUpdate()
        .name("Test Operation")
        .operationId(connectionRead.getOperationIds().get(0));
    final OperationUpdate operationUpdate = WebBackendConnectionsHandler.toOperationUpdate(operationCreateOrUpdate);
    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expected.getSyncCatalog())
        .operations(List.of(operationCreateOrUpdate));

    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());

    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of());
    when(connectionsHandler.getDiff(any(), any(), any(), any())).thenReturn(catalogDiff);
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(expected.getConnectionId());
    when(stateHandler.getState(connectionIdRequestBody)).thenReturn(new ConnectionState().stateType(ConnectionStateType.LEGACY));

    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead()
            .connectionId(expected.getConnectionId())
            .sourceId(expected.getSourceId())
            .operationIds(connectionRead.getOperationIds())
            .breakingChange(false));
    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(
        new ConnectionRead()
            .connectionId(expected.getConnectionId())
            .sourceId(expected.getSourceId())
            .destinationId(expected.getDestinationId())
            .operationIds(connectionRead.getOperationIds())
            .name(expected.getName())
            .namespaceDefinition(expected.getNamespaceDefinition())
            .namespaceFormat(expected.getNamespaceFormat())
            .prefix(expected.getPrefix())
            .syncCatalog(expected.getSyncCatalog())
            .status(expected.getStatus())
            .schedule(expected.getSchedule()).breakingChange(false));
    when(operationsHandler.updateOperation(operationUpdate)).thenReturn(new OperationRead().operationId(operationUpdate.getOperationId()));
    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);

    final WebBackendConnectionRead actualConnectionRead = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(connectionRead.getOperationIds(), actualConnectionRead.getOperationIds());
    verify(operationsHandler, times(1)).updateOperation(operationUpdate);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateConnectionWithUpdatedSchemaPerStream(final Boolean useRefresh)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    when(actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(any()))
        .thenReturn(new ActorDefinitionVersionRead().supportsRefreshes(useRefresh));

    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog());

    // state is per-stream
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(expected.getConnectionId());
    when(stateHandler.getState(connectionIdRequestBody)).thenReturn(new ConnectionState().stateType(ConnectionStateType.STREAM));
    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());

    final StreamDescriptor streamDescriptorAdd = new StreamDescriptor().name("addStream");
    final StreamDescriptor streamDescriptorRemove = new StreamDescriptor().name("removeStream");
    final StreamDescriptor streamDescriptorUpdate = new StreamDescriptor().name("updateStream");

    final StreamTransform streamTransformAdd =
        new StreamTransform().streamDescriptor(streamDescriptorAdd).transformType(TransformTypeEnum.ADD_STREAM);
    final StreamTransform streamTransformRemove =
        new StreamTransform().streamDescriptor(streamDescriptorRemove).transformType(TransformTypeEnum.REMOVE_STREAM);
    final StreamTransform streamTransformUpdate =
        new StreamTransform().streamDescriptor(streamDescriptorUpdate).transformType(TransformTypeEnum.UPDATE_STREAM)
            .updateStream(new StreamTransformUpdateStream());

    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of(streamTransformAdd, streamTransformRemove, streamTransformUpdate));
    when(connectionsHandler.getDiff(any(), any(), any(), any())).thenReturn(catalogDiff);
    when(connectionsHandler.getConfigurationDiff(any(), any())).thenReturn(Set.of(new StreamDescriptor().name("configUpdateStream")));

    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead().connectionId(expected.getConnectionId()).breakingChange(false));
    final ConnectionRead connectionRead = new ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule())
        .breakingChange(false);
    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(connectionRead);
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(connectionRead);

    final ManualOperationResult successfulResult = new ManualOperationResult(null, null, null);
    when(eventRunner.resetConnection(any(), any())).thenReturn(successfulResult);
    when(eventRunner.startNewManualSync(any())).thenReturn(successfulResult);

    when(catalogService.getMostRecentActorCatalogForSource(any())).thenReturn(Optional.of(new ActorCatalog().withCatalog(Jsons.emptyObject())));

    final WebBackendConnectionRead result = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(expectedWithNewSchema.getSyncCatalog(), result.getSyncCatalog());

    verify(destinationCatalogGenerator).generateDestinationCatalog(catalogConverter.toConfiguredInternal(expectedWithNewSchema.getSyncCatalog()));
    verify(destinationCatalogGenerator).generateDestinationCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());

    final ConnectionIdRequestBody connectionId = new ConnectionIdRequestBody().connectionId(result.getConnectionId());
    verify(schedulerHandler, times(0)).resetConnection(connectionId);
    verify(schedulerHandler, times(0)).syncConnection(connectionId);
    verify(connectionsHandler, times(1)).updateConnection(any(), any(), any());
    final InOrder orderVerifier = inOrder(eventRunner);
    if (useRefresh) {
      orderVerifier.verify(eventRunner, times(1)).refreshConnectionAsync(connectionId.getConnectionId(),
          List.of(new io.airbyte.config.StreamDescriptor().withName("addStream"),
              new io.airbyte.config.StreamDescriptor().withName("updateStream"),
              new io.airbyte.config.StreamDescriptor().withName("configUpdateStream"),
              new io.airbyte.config.StreamDescriptor().withName("removeStream")),
          RefreshType.MERGE);
    } else {
      orderVerifier.verify(eventRunner, times(1)).resetConnectionAsync(connectionId.getConnectionId(),
          List.of(new io.airbyte.config.StreamDescriptor().withName("addStream"),
              new io.airbyte.config.StreamDescriptor().withName("updateStream"),
              new io.airbyte.config.StreamDescriptor().withName("configUpdateStream"),
              new io.airbyte.config.StreamDescriptor().withName("removeStream")));
    }
  }

  @Test
  void testUpdateConnectionNoStreamsToReset()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog());

    // state is per-stream
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(expected.getConnectionId());
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog();
    when(stateHandler.getState(connectionIdRequestBody)).thenReturn(new ConnectionState().stateType(ConnectionStateType.STREAM));
    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(configuredAirbyteCatalog);

    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of());
    when(connectionsHandler.getDiff(any(), any(), any(), any())).thenReturn(catalogDiff);

    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead().connectionId(expected.getConnectionId()));
    final ConnectionRead connectionRead = new ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule()).breakingChange(false);
    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(connectionRead);
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(connectionRead);

    final WebBackendConnectionRead result = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(expectedWithNewSchema.getSyncCatalog(), result.getSyncCatalog());

    final ConnectionIdRequestBody connectionId = new ConnectionIdRequestBody().connectionId(result.getConnectionId());

    verify(connectionsHandler).getDiff(expected.getSyncCatalog(), expectedWithNewSchema.getSyncCatalog(),
        catalogConverter.toConfiguredInternal(result.getSyncCatalog()), expected.getConnectionId());
    verify(connectionsHandler).getConfigurationDiff(expected.getSyncCatalog(), expectedWithNewSchema.getSyncCatalog());
    verify(schedulerHandler, times(0)).resetConnection(connectionId);
    verify(schedulerHandler, times(0)).syncConnection(connectionId);
    verify(connectionsHandler, times(1)).updateConnection(any(), any(), any());
    final InOrder orderVerifier = inOrder(eventRunner);
    orderVerifier.verify(eventRunner, times(0)).resetConnection(eq(connectionId.getConnectionId()), any());
    orderVerifier.verify(eventRunner, times(0)).startNewManualSync(connectionId.getConnectionId());
  }

  @Test
  void testUpdateConnectionWithSkipReset()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .skipReset(true);

    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());
    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead().connectionId(expected.getConnectionId()).sourceId(expected.getSourceId()));
    final ConnectionRead connectionRead = new ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule())
        .breakingChange(false);
    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(connectionRead);

    final WebBackendConnectionRead result = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(expectedWithNewSchema.getSyncCatalog(), result.getSyncCatalog());

    final ConnectionIdRequestBody connectionId = new ConnectionIdRequestBody().connectionId(result.getConnectionId());
    verify(schedulerHandler, times(0)).resetConnection(connectionId);
    verify(schedulerHandler, times(0)).syncConnection(connectionId);
    verify(connectionsHandler, times(0)).getDiff(any(), any(), any(), any());
    verify(connectionsHandler, times(1)).updateConnection(any(), any(), any());
    verify(eventRunner, times(0)).resetConnection(any(), any());
  }

  @Test
  void testUpdateConnectionFixingBreakingSchemaChange()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final WebBackendConnectionUpdate updateBody = new WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .skipReset(false)
        .connectionId(expected.getConnectionId());

    final UUID sourceId = sourceRead.getSourceId();

    // existing connection has a breaking change
    when(connectionsHandler.getConnection(expected.getConnectionId())).thenReturn(
        new ConnectionRead().connectionId(expected.getConnectionId()).breakingChange(true).sourceId(sourceId));

    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of());

    when(catalogService.getMostRecentActorCatalogForSource(sourceId)).thenReturn(Optional.of(new ActorCatalog().withCatalog(Jsons.deserialize(
        "{\"streams\": [{\"name\": \"cat_names\", "
            + "\"namespace\": \"public\", "
            + "\"json_schema\": {\"type\": \"object\", \"properties\": {\"id\": {\"type\": \"number\", \"airbyte_type\": \"integer\"}}}}]}"))));
    when(connectionsHandler.getDiff(any(), any(), any(), any())).thenReturn(catalogDiff, catalogDiff);

    when(connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()))
        .thenReturn(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog());
    when(operationsHandler.listOperationsForConnection(any())).thenReturn(operationReadList);

    final ConnectionRead connectionRead = new ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expectedWithNewSchema.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule())
        .breakingChange(false);

    when(connectionsHandler.updateConnection(any(), any(), any())).thenReturn(connectionRead);

    final WebBackendConnectionRead result = wbHandler.webBackendUpdateConnection(updateBody);

    assertEquals(expectedWithNewSchema.getSyncCatalog(), result.getSyncCatalog());

    final ConnectionIdRequestBody connectionId = new ConnectionIdRequestBody().connectionId(result.getConnectionId());
    final ArgumentCaptor<ConnectionUpdate> expectedArgumentCaptor = ArgumentCaptor.forClass(ConnectionUpdate.class);
    verify(connectionsHandler, times(1)).updateConnection(expectedArgumentCaptor.capture(), any(), any());
    final List<ConnectionUpdate> connectionUpdateValues = expectedArgumentCaptor.getAllValues();
    // Expect the ConnectionUpdate object to have breakingChange: false
    assertEquals(false, connectionUpdateValues.get(0).getBreakingChange());

    verify(schedulerHandler, times(0)).resetConnection(connectionId);
    verify(schedulerHandler, times(0)).syncConnection(connectionId);
    verify(connectionsHandler, times(2)).getDiff(any(), any(), any(), any());
    verify(connectionsHandler, times(1)).updateConnection(any(), any(), any());
  }

  @Test
  void testUpdateSchemaWithDiscoveryFromEmpty() {
    final AirbyteCatalog original = new AirbyteCatalog().streams(List.of());
    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    discovered.getStreams().get(0).getStream()
        .name(STREAM1)
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    discovered.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1);

    final AirbyteCatalog expected = ConnectionHelpers.generateBasicApiCatalog();
    expected.getStreams().get(0).getStream()
        .name(STREAM1)
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    expected.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1)
        .selected(false)
        .suggested(false)
        .selectedFields(List.of());

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    assertEquals(expected, actual);
  }

  @Test
  void testUpdateSchemaWithDiscoveryResetStream() {
    final AirbyteCatalog original = ConnectionHelpers.generateBasicApiCatalog();
    original.getStreams().get(0).getStream()
        .name("random-stream")
        .defaultCursorField(List.of(FIELD1))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(
            Field.of(FIELD1, JsonSchemaType.NUMBER),
            Field.of(FIELD2, JsonSchemaType.NUMBER),
            Field.of(FIELD5, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    original.getStreams().get(0).getConfig()
        .syncMode(SyncMode.INCREMENTAL)
        .cursorField(List.of(FIELD1))
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .primaryKey(Collections.emptyList())
        .aliasName("random_stream");

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    discovered.getStreams().get(0).getStream()
        .name(STREAM1)
        .defaultCursorField(List.of(FIELD3))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    discovered.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1);

    final AirbyteCatalog expected = ConnectionHelpers.generateBasicApiCatalog();
    expected.getStreams().get(0).getStream()
        .name(STREAM1)
        .defaultCursorField(List.of(FIELD3))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    expected.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1)
        .selected(false)
        .suggested(false)
        .selectedFields(List.of());

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    assertEquals(expected, actual);
  }

  @Test
  void testUpdateSchemaWithDiscoveryMergeNewStream() {
    final AirbyteCatalog original = ConnectionHelpers.generateBasicApiCatalog();
    original.getStreams().get(0).getStream()
        .name(STREAM1)
        .defaultCursorField(List.of(FIELD1))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(
            Field.of(FIELD1, JsonSchemaType.NUMBER),
            Field.of(FIELD2, JsonSchemaType.NUMBER),
            Field.of(FIELD5, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    original.getStreams().get(0).getConfig()
        .syncMode(SyncMode.INCREMENTAL)
        .cursorField(List.of(FIELD1))
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .primaryKey(Collections.emptyList())
        .aliasName("renamed_stream");

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    discovered.getStreams().get(0).getStream()
        .name(STREAM1)
        .defaultCursorField(List.of(FIELD3))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    discovered.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1);
    final AirbyteStreamAndConfiguration newStream = ConnectionHelpers.generateBasicApiCatalog().getStreams().get(0);
    newStream.getStream()
        .name(STREAM2)
        .defaultCursorField(List.of(FIELD5))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    newStream.getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM2);
    discovered.getStreams().add(newStream);

    final AirbyteCatalog expected = ConnectionHelpers.generateBasicApiCatalog();
    expected.getStreams().get(0).getStream()
        .name(STREAM1)
        .defaultCursorField(List.of(FIELD3))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    expected.getStreams().get(0).getConfig()
        .syncMode(SyncMode.INCREMENTAL)
        .cursorField(List.of(FIELD1))
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .primaryKey(Collections.emptyList())
        .aliasName("renamed_stream")
        .selected(true)
        .suggested(false)
        .selectedFields(List.of());
    final AirbyteStreamAndConfiguration expectedNewStream = ConnectionHelpers.generateBasicApiCatalog().getStreams().get(0);
    expectedNewStream.getStream()
        .name(STREAM2)
        .defaultCursorField(List.of(FIELD5))
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    expectedNewStream.getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM2)
        .selected(false)
        .suggested(false)
        .selectedFields(List.of());
    expected.getStreams().add(expectedNewStream);

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    assertEquals(expected, actual);
  }

  @Test
  void testUpdateSchemaWithDiscoveryWithChangedSourceDefinedPK() {
    final AirbyteCatalog original = ConnectionHelpers.generateBasicApiCatalog();
    original.getStreams().getFirst().getStream()
        .sourceDefinedPrimaryKey(List.of(List.of(FIELD1)));
    original.getStreams().getFirst().getConfig()
        .primaryKey(List.of(List.of(FIELD1)));

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    discovered.getStreams().getFirst().getStream()
        .sourceDefinedPrimaryKey(List.of(List.of(FIELD2)));
    discovered.getStreams().getFirst().getConfig()
        .primaryKey(List.of(List.of(FIELD2)));

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    // Use new value for source-defined PK
    assertEquals(List.of(List.of(FIELD2)), actual.getStreams().getFirst().getConfig().getPrimaryKey());
  }

  @Test
  void testUpdateSchemaWithDiscoveryWithNoSourceDefinedPK() {
    final AirbyteCatalog original = ConnectionHelpers.generateBasicApiCatalog();
    original.getStreams().getFirst().getConfig()
        .primaryKey(List.of(List.of(FIELD1)));

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    assertNotEquals(original.getStreams().getFirst().getConfig().getPrimaryKey(), discovered.getStreams().getFirst().getConfig().getPrimaryKey());

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    // Keep previously-configured PK
    assertEquals(List.of(List.of(FIELD1)), actual.getStreams().getFirst().getConfig().getPrimaryKey());
  }

  @Test
  void testUpdateSchemaWithDiscoveryWithHashedField() {
    final List<SelectedFieldInfo> hashedFields = List.of(new SelectedFieldInfo().fieldPath(List.of(SECOND_FIELD_NAME)));

    final AirbyteCatalog original = ConnectionHelpers.generateApiCatalogWithTwoFields();
    original.getStreams().getFirst().getConfig().setHashedFields(hashedFields);

    final AirbyteCatalog discovered = ConnectionHelpers.generateApiCatalogWithTwoFields();

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    // Configure hashed fields
    assertEquals(hashedFields, actual.getStreams().getFirst().getConfig().getHashedFields());
  }

  @Test
  void testUpdateSchemaWithDiscoveryWithRemovedHashedField() {
    final AirbyteCatalog original = ConnectionHelpers.generateApiCatalogWithTwoFields();
    original.getStreams().getFirst().getConfig().setHashedFields(List.of(new SelectedFieldInfo().fieldPath(List.of(SECOND_FIELD_NAME))));

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    // Remove hashed field
    assertTrue(actual.getStreams().getFirst().getConfig().getHashedFields().isEmpty());
  }

  @Test
  void testUpdateSchemaWithNamespacedStreams() {
    final AirbyteCatalog original = ConnectionHelpers.generateBasicApiCatalog();
    final AirbyteStreamAndConfiguration stream1Config = original.getStreams().get(0);
    final AirbyteStream stream1 = stream1Config.getStream();
    final AirbyteStream stream2 = new AirbyteStream()
        .name(stream1.getName())
        .namespace("second_namespace")
        .jsonSchema(stream1.getJsonSchema())
        .defaultCursorField(stream1.getDefaultCursorField())
        .supportedSyncModes(stream1.getSupportedSyncModes())
        .sourceDefinedCursor(stream1.getSourceDefinedCursor())
        .sourceDefinedPrimaryKey(stream1.getSourceDefinedPrimaryKey());
    final AirbyteStreamAndConfiguration stream2Config = new AirbyteStreamAndConfiguration()
        .config(stream1Config.getConfig())
        .stream(stream2);
    original.getStreams().add(stream2Config);

    final AirbyteCatalog discovered = ConnectionHelpers.generateBasicApiCatalog();
    discovered.getStreams().get(0).getStream()
        .name(STREAM1)
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    discovered.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1);

    final AirbyteCatalog expected = ConnectionHelpers.generateBasicApiCatalog();
    expected.getStreams().get(0).getStream()
        .name(STREAM1)
        .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH));
    expected.getStreams().get(0).getConfig()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM1)
        .selected(false)
        .suggested(false)
        .selectedFields(List.of());

    final AirbyteCatalog actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered);

    assertEquals(expected, actual);
  }

  @Test
  void testGetStreamsToReset() {
    final StreamTransform streamTransformAdd =
        new StreamTransform().transformType(TransformTypeEnum.ADD_STREAM).streamDescriptor(new StreamDescriptor().name("added_stream"));
    final StreamTransform streamTransformRemove =
        new StreamTransform().transformType(TransformTypeEnum.REMOVE_STREAM).streamDescriptor(new StreamDescriptor().name("removed_stream"));
    final StreamTransform streamTransformUpdate =
        new StreamTransform().transformType(TransformTypeEnum.UPDATE_STREAM).streamDescriptor(new StreamDescriptor().name("updated_stream"));
    final CatalogDiff catalogDiff = new CatalogDiff().transforms(List.of(streamTransformAdd, streamTransformRemove, streamTransformUpdate));
    final List<StreamDescriptor> resultList = WebBackendConnectionsHandler.getStreamsToReset(catalogDiff);
    assertTrue(
        resultList.stream().anyMatch(
            streamDescriptor -> "added_stream".equalsIgnoreCase(streamDescriptor.getName())));
    assertTrue(
        resultList.stream().anyMatch(
            streamDescriptor -> "removed_stream".equalsIgnoreCase(streamDescriptor.getName())));
    assertTrue(
        resultList.stream().anyMatch(
            streamDescriptor -> "updated_stream".equalsIgnoreCase(streamDescriptor.getName())));
  }

  @Test
  void testGetSchemaChangeNoChange() {
    final ConnectionRead connectionReadNotBreaking = new ConnectionRead().breakingChange(false);

    assertEquals(SchemaChange.NO_CHANGE,
        WebBackendConnectionsHandler.getSchemaChange(null, Optional.of(UUID.randomUUID()), Optional.of(new ActorCatalogFetchEvent())));
    assertEquals(SchemaChange.NO_CHANGE,
        WebBackendConnectionsHandler.getSchemaChange(connectionReadNotBreaking, Optional.empty(), Optional.of(new ActorCatalogFetchEvent())));

    final UUID catalogId = UUID.randomUUID();

    assertEquals(SchemaChange.NO_CHANGE, WebBackendConnectionsHandler.getSchemaChange(connectionReadNotBreaking, Optional.of(catalogId),
        Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(catalogId))));
  }

  @Test
  void testGetSchemaChangeBreaking() {
    final UUID sourceId = UUID.randomUUID();
    final ConnectionRead connectionReadWithSourceId = new ConnectionRead().sourceCatalogId(UUID.randomUUID()).sourceId(sourceId).breakingChange(true);

    assertEquals(SchemaChange.BREAKING, WebBackendConnectionsHandler.getSchemaChange(connectionReadWithSourceId,
        Optional.of(UUID.randomUUID()), Optional.empty()));
  }

  @Test
  void testGetSchemaChangeNotBreaking() {
    final UUID catalogId = UUID.randomUUID();
    final UUID differentCatalogId = UUID.randomUUID();
    final ConnectionRead connectionReadWithSourceId =
        new ConnectionRead().breakingChange(false);

    assertEquals(SchemaChange.NON_BREAKING, WebBackendConnectionsHandler.getSchemaChange(connectionReadWithSourceId,
        Optional.of(catalogId), Optional.of(new ActorCatalogFetchEvent().withActorCatalogId(differentCatalogId))));
  }

}
