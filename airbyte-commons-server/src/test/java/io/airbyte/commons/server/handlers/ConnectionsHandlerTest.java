/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.helpers.ConnectionHelpers.FIELD_NAME;
import static io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME;
import static io.airbyte.config.Job.REPLICATION_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConfiguredStreamMapper;
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleData;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnitEnum;
import io.airbyte.api.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.model.generated.ConnectionScheduleType;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncStatus;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldAdd;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobSyncResultRead;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.NamespaceDefinitionType;
import io.airbyte.api.model.generated.ResourceRequirements;
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamMapperType;
import io.airbyte.api.model.generated.StreamStats;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.problems.model.generated.MapperValidationProblemResponse;
import io.airbyte.api.problems.model.generated.ProblemMapperErrorData;
import io.airbyte.api.problems.model.generated.ProblemMapperErrorDataMapper;
import io.airbyte.api.problems.throwable.generated.MapperValidationProblem;
import io.airbyte.commons.converters.ConnectionHelper;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper;
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper;
import io.airbyte.commons.server.handlers.helpers.MapperSecretHelper;
import io.airbyte.commons.server.handlers.helpers.NotificationHelper;
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.CronExpressionHelper;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.validation.CatalogValidator;
import io.airbyte.commons.server.validation.ValidationError;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.AttemptWithJobInfo;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.Cron;
import io.airbyte.config.DataType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FieldSelectionData;
import io.airbyte.config.Geography;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobOutput.OutputType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.MapperConfig;
import io.airbyte.config.MapperOperationName;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.RefreshStream;
import io.airbyte.config.RefreshStream.RefreshType;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.Schedule;
import io.airbyte.config.Schedule.TimeUnit;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.NonBreakingChangesPreference;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.mapper.configs.HashingMapperConfig;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.config.persistence.StreamGenerationRepository;
import io.airbyte.config.persistence.domain.Generation;
import io.airbyte.config.persistence.helper.CatalogGenerationSetter;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.StreamStatusesService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.ResetStreamsStateWhenDisabled;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.mappers.helpers.MapperHelperKt;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.CatalogGenerationResult;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.MapperError;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.MapperErrorType;
import io.airbyte.mappers.transformations.HashingMapper;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class ConnectionsHandlerTest {

  private static final String PRESTO_TO_HUDI = "presto to hudi";
  private static final String PRESTO_TO_HUDI_PREFIX = "presto_to_hudi";
  private static final String SOURCE_TEST = "source-test";
  private static final String DESTINATION_TEST = "destination-test";
  private static final String CURSOR1 = "cursor1";
  private static final String CURSOR2 = "cursor2";
  private static final String PK1 = "pk1";
  private static final String PK2 = "pk2";
  private static final String PK3 = "pk3";
  private static final String STREAM1 = "stream1";
  private static final String STREAM2 = "stream2";
  private static final String AZKABAN_USERS = "azkaban_users";
  private static final String CRON_TIMEZONE_UTC = "UTC";
  private static final String TIMEZONE_LOS_ANGELES = "America/Los_Angeles";
  private static final String CRON_EXPRESSION = "0 0 */2 * * ?";
  private static final String STREAM_SELECTION_DATA = "null/users-data0";
  private JobPersistence jobPersistence;
  private Supplier<UUID> uuidGenerator;
  private ConnectionsHandler connectionsHandler;
  private MatchSearchHandler matchSearchHandler;
  private UUID workspaceId;
  private UUID sourceId;
  private UUID destinationId;
  private UUID sourceDefinitionId;
  private UUID destinationDefinitionId;
  private SourceConnection source;
  private DestinationConnection destination;
  private StandardSync standardSync;
  private StandardSync standardSync2;
  private StandardSync standardSyncDeleted;
  private UUID connectionId;
  private UUID connection2Id;
  private UUID operationId;
  private UUID otherOperationId;
  private WorkspaceHelper workspaceHelper;
  private TrackingClient trackingClient;
  private EventRunner eventRunner;
  private ConnectionHelper connectionHelper;
  private TestClient featureFlagClient;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;
  private ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;

  private JsonSchemaValidator jsonSchemaValidator;
  private JsonSecretsProcessor secretsProcessor;
  private ConfigurationUpdate configurationUpdate;
  private OAuthConfigSupplier oAuthConfigSupplier;
  private DestinationService destinationService;

  private SecretsRepositoryReader secretsRepositoryReader;
  private SourceService sourceService;
  private WorkspaceService workspaceService;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private MapperSecretHelper mapperSecretHelper;

  private DestinationHandler destinationHandler;
  private SourceHandler sourceHandler;
  private StreamRefreshesHandler streamRefreshesHandler;
  private Job job;
  private StreamGenerationRepository streamGenerationRepository;
  private CatalogGenerationSetter catalogGenerationSetter;
  private CatalogValidator catalogValidator;
  private NotificationHelper notificationHelper;
  private StreamStatusesService streamStatusesService;
  private ConnectionTimelineEventService connectionTimelineEventService;
  private ConnectionTimelineEventHelper connectionTimelineEventHelper;
  private StatePersistence statePersistence;
  private CatalogService catalogService;
  private ConnectionService connectionService;
  private DestinationCatalogGenerator destinationCatalogGenerator;
  private ConnectionScheduleHelper connectionSchedulerHelper;
  private final CatalogConverter catalogConverter = new CatalogConverter(new FieldGenerator(), Collections.singletonList(new HashingMapper()));
  private final ApplySchemaChangeHelper applySchemaChangeHelper = new ApplySchemaChangeHelper(catalogConverter);
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(catalogConverter);
  private final CronExpressionHelper cronExpressionHelper = new CronExpressionHelper();

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException, JsonValidationException, ConfigNotFoundException {

    workspaceId = UUID.randomUUID();
    sourceId = UUID.randomUUID();
    destinationId = UUID.randomUUID();
    sourceDefinitionId = UUID.randomUUID();
    destinationDefinitionId = UUID.randomUUID();
    connectionId = UUID.randomUUID();
    connection2Id = UUID.randomUUID();
    operationId = UUID.randomUUID();
    otherOperationId = UUID.randomUUID();
    source = new SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(workspaceId)
        .withName("presto");
    destination = new DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(workspaceId)
        .withName("hudi")
        .withConfiguration(Jsons.jsonNode(Collections.singletonMap("apiKey", "123-abc")));
    standardSync = new StandardSync()
        .withConnectionId(connectionId)
        .withName(PRESTO_TO_HUDI)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(PRESTO_TO_HUDI_PREFIX)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog())
        .withFieldSelectionData(new FieldSelectionData().withAdditionalProperty(STREAM_SELECTION_DATA, false))
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(operationId))
        .withManual(false)
        .withSchedule(ConnectionHelpers.generateBasicSchedule())
        .withScheduleType(ScheduleType.BASIC_SCHEDULE)
        .withScheduleData(ConnectionHelpers.generateBasicScheduleData())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
        .withSourceCatalogId(UUID.randomUUID())
        .withGeography(Geography.AUTO)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(true)
        .withBreakingChange(false)
        .withBackfillPreference(StandardSync.BackfillPreference.ENABLED);
    standardSync2 = new StandardSync()
        .withConnectionId(connection2Id)
        .withName(PRESTO_TO_HUDI)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(PRESTO_TO_HUDI_PREFIX)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog())
        .withFieldSelectionData(new FieldSelectionData().withAdditionalProperty(STREAM_SELECTION_DATA, false))
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(operationId))
        .withManual(false)
        .withSchedule(ConnectionHelpers.generateBasicSchedule())
        .withScheduleType(ScheduleType.BASIC_SCHEDULE)
        .withScheduleData(ConnectionHelpers.generateBasicScheduleData())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
        .withSourceCatalogId(UUID.randomUUID())
        .withGeography(Geography.AUTO)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(true)
        .withBreakingChange(false);
    standardSyncDeleted = new StandardSync()
        .withConnectionId(connectionId)
        .withName("presto to hudi2")
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi2")
        .withStatus(StandardSync.Status.DEPRECATED)
        .withCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog())
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(operationId))
        .withManual(false)
        .withSchedule(ConnectionHelpers.generateBasicSchedule())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
        .withGeography(Geography.US);

    jobPersistence = mock(JobPersistence.class);
    streamRefreshesHandler = mock(StreamRefreshesHandler.class);
    catalogService = mock(CatalogService.class);
    uuidGenerator = mock(Supplier.class);
    workspaceHelper = mock(WorkspaceHelper.class);
    trackingClient = mock(TrackingClient.class);
    eventRunner = mock(EventRunner.class);
    connectionHelper = mock(ConnectionHelper.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    connectorDefinitionSpecificationHandler = mock(ConnectorDefinitionSpecificationHandler.class);
    jsonSchemaValidator = mock(JsonSchemaValidator.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    configurationUpdate = mock(ConfigurationUpdate.class);
    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    destinationService = mock(DestinationService.class);
    connectionService = mock(ConnectionService.class);

    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    sourceService = mock(SourceService.class);
    workspaceService = mock(WorkspaceService.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    streamStatusesService = mock(StreamStatusesService.class);
    connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    connectionTimelineEventHelper = mock(ConnectionTimelineEventHelper.class);
    statePersistence = mock(StatePersistence.class);
    mapperSecretHelper = mock(MapperSecretHelper.class);

    featureFlagClient = mock(TestClient.class);

    destinationHandler =
        new DestinationHandler(
            jsonSchemaValidator,
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
    sourceHandler = new SourceHandler(
        catalogService,
        secretsRepositoryReader,
        jsonSchemaValidator,
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

    connectionSchedulerHelper = new ConnectionScheduleHelper(apiPojoConverters, cronExpressionHelper, featureFlagClient, workspaceHelper);
    matchSearchHandler =
        new MatchSearchHandler(destinationHandler, sourceHandler, sourceService, destinationService, connectionService, apiPojoConverters);
    featureFlagClient = mock(TestClient.class);
    job = mock(Job.class);
    streamGenerationRepository = mock(StreamGenerationRepository.class);
    catalogGenerationSetter = mock(CatalogGenerationSetter.class);
    catalogValidator = mock(CatalogValidator.class);
    notificationHelper = mock(NotificationHelper.class);
    when(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(sourceId)).thenReturn(workspaceId);
    when(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)).thenReturn(workspaceId);
    when(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(workspaceId);
    when(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(otherOperationId)).thenReturn(workspaceId);

    destinationCatalogGenerator = mock(DestinationCatalogGenerator.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new CatalogGenerationResult(new ConfiguredAirbyteCatalog(), Map.of()));

    when(mapperSecretHelper.maskMapperSecrets(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(mapperSecretHelper.createAndReplaceMapperSecrets(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    when(mapperSecretHelper.updateAndReplaceMapperSecrets(any(), any(), any())).thenAnswer(invocation -> invocation.getArgument(2));
  }

  @Nested
  class UnMockedConnectionHelper {

    @BeforeEach
    void setUp() throws JsonValidationException, ConfigNotFoundException, IOException {
      connectionsHandler = new ConnectionsHandler(
          streamRefreshesHandler,
          jobPersistence,
          catalogService,
          uuidGenerator,
          workspaceHelper,
          trackingClient,
          eventRunner,
          connectionHelper,
          featureFlagClient,
          actorDefinitionVersionHelper,
          connectorDefinitionSpecificationHandler,
          streamGenerationRepository,
          catalogGenerationSetter,
          catalogValidator,
          notificationHelper,
          streamStatusesService,
          connectionTimelineEventService,
          connectionTimelineEventHelper,
          statePersistence,
          sourceService,
          destinationService,
          connectionService,
          workspaceService,
          destinationCatalogGenerator,
          catalogConverter,
          applySchemaChangeHelper,
          apiPojoConverters,
          connectionSchedulerHelper,
          mapperSecretHelper);

      when(uuidGenerator.get()).thenReturn(standardSync.getConnectionId());
      final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
          .withName(SOURCE_TEST)
          .withSourceDefinitionId(UUID.randomUUID());
      final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
          .withName(DESTINATION_TEST)
          .withDestinationDefinitionId(UUID.randomUUID());
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);
      when(sourceService.getSourceDefinitionFromConnection(standardSync.getConnectionId())).thenReturn(
          sourceDefinition);
      when(destinationService.getDestinationDefinitionFromConnection(standardSync.getConnectionId())).thenReturn(
          destinationDefinition);
      when(sourceService.getSourceConnection(source.getSourceId()))
          .thenReturn(source);
      when(destinationService.getDestinationConnection(destination.getDestinationId()))
          .thenReturn(destination);
      when(connectionService.getStandardSync(connectionId)).thenReturn(standardSync);
      when(jobPersistence.getLastReplicationJob(connectionId)).thenReturn(Optional.of(job));
      when(jobPersistence.getFirstReplicationJob(connectionId)).thenReturn(Optional.of(job));
    }

    @Test
    void testGetConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);

      final ConnectionRead actualConnectionRead = connectionsHandler.getConnection(standardSync.getConnectionId());

      assertEquals(ConnectionHelpers.generateExpectedConnectionRead(standardSync), actualConnectionRead);
    }

    @Test
    void testGetConnectionForJob() throws JsonValidationException, ConfigNotFoundException, IOException {
      final Long jobId = 456L;

      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);
      when(jobPersistence.getJob(jobId)).thenReturn(new Job(
          jobId,
          ConfigType.SYNC,
          null,
          null,
          null,
          null,
          null,
          0,
          0));
      final List<Generation> generations = List.of(new Generation("name", null, 1));
      when(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.getConnectionId())).thenReturn(generations);
      when(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.getCatalog(),
          jobId,
          List.of(),
          generations)).thenReturn(standardSync.getCatalog());

      final ConnectionRead actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.getConnectionId(), jobId);

      assertEquals(ConnectionHelpers.generateExpectedConnectionRead(standardSync), actualConnectionRead);
    }

    @Test
    void testGetConnectionForJobWithRefresh() throws JsonValidationException, ConfigNotFoundException, IOException {
      final Long jobId = 456L;

      final List<RefreshStream> refreshStreamDescriptors =
          List.of(new RefreshStream().withRefreshType(RefreshType.TRUNCATE)
              .withStreamDescriptor(new io.airbyte.config.StreamDescriptor().withName("name")));

      final JobConfig config = new JobConfig()
          .withRefresh(new RefreshConfig().withStreamsToRefresh(refreshStreamDescriptors));

      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);
      when(jobPersistence.getJob(jobId)).thenReturn(new Job(
          jobId,
          ConfigType.REFRESH,
          null,
          config,
          null,
          null,
          null,
          0,
          0));
      final List<Generation> generations = List.of(new Generation("name", null, 1));
      when(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.getConnectionId())).thenReturn(generations);
      when(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.getCatalog(),
          jobId,
          refreshStreamDescriptors,
          generations)).thenReturn(standardSync.getCatalog());

      final ConnectionRead actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.getConnectionId(), jobId);

      assertEquals(ConnectionHelpers.generateExpectedConnectionRead(standardSync), actualConnectionRead);
    }

    @Test
    void testGetConnectionForClearJob() throws JsonValidationException, ConfigNotFoundException, IOException {
      final Long jobId = 456L;

      final List<io.airbyte.config.StreamDescriptor> clearedStreamDescriptors =
          List.of(new io.airbyte.config.StreamDescriptor().withName("name"));

      final JobConfig config = new JobConfig()
          .withResetConnection(new JobResetConnectionConfig().withResetSourceConfiguration(
              new ResetSourceConfiguration().withStreamsToReset(clearedStreamDescriptors)));

      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);
      when(jobPersistence.getJob(jobId)).thenReturn(new Job(
          jobId,
          ConfigType.RESET_CONNECTION,
          null,
          config,
          null,
          null,
          null,
          0,
          0));
      final List<Generation> generations = List.of(new Generation("name", null, 1));
      when(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.getConnectionId())).thenReturn(generations);
      when(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformationForClear(
          standardSync.getCatalog(),
          jobId,
          Set.copyOf(clearedStreamDescriptors),
          generations)).thenReturn(standardSync.getCatalog());

      final ConnectionRead actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.getConnectionId(), jobId);

      assertEquals(ConnectionHelpers.generateExpectedConnectionRead(standardSync), actualConnectionRead);
    }

    @Test
    void testListConnectionsForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
      when(connectionService.listWorkspaceStandardSyncs(source.getWorkspaceId(), false))
          .thenReturn(Lists.newArrayList(standardSync));
      when(connectionService.listWorkspaceStandardSyncs(source.getWorkspaceId(), true))
          .thenReturn(Lists.newArrayList(standardSync, standardSyncDeleted));
      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);

      final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(source.getWorkspaceId());
      final ConnectionReadList actualConnectionReadList = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(
          ConnectionHelpers.generateExpectedConnectionRead(standardSync),
          actualConnectionReadList.getConnections().get(0));

      final ConnectionReadList actualConnectionReadListWithDeleted = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody, true);
      final List<ConnectionRead> connections = actualConnectionReadListWithDeleted.getConnections();
      assertEquals(2, connections.size());
      assertEquals(apiPojoConverters.internalToConnectionRead(standardSync), connections.get(0));
      assertEquals(apiPojoConverters.internalToConnectionRead(standardSyncDeleted), connections.get(1));

    }

    @Test
    void testListConnections() throws JsonValidationException, ConfigNotFoundException, IOException {
      when(connectionService.listStandardSyncs())
          .thenReturn(Lists.newArrayList(standardSync));
      when(sourceService.getSourceConnection(source.getSourceId()))
          .thenReturn(source);
      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);

      final ConnectionReadList actualConnectionReadList = connectionsHandler.listConnections();

      assertEquals(
          ConnectionHelpers.generateExpectedConnectionRead(standardSync),
          actualConnectionReadList.getConnections().get(0));
    }

    @Test
    void testListConnectionsByActorDefinition() throws IOException {
      when(connectionService.listConnectionsByActorDefinitionIdAndType(sourceDefinitionId, ActorType.SOURCE.value(), false, true))
          .thenReturn(Lists.newArrayList(standardSync));
      when(connectionService.listConnectionsByActorDefinitionIdAndType(destinationDefinitionId, ActorType.DESTINATION.value(), false, true))
          .thenReturn(Lists.newArrayList(standardSync2));

      final ConnectionReadList connectionReadListForSourceDefinitionId = connectionsHandler.listConnectionsForActorDefinition(
          new ActorDefinitionRequestBody()
              .actorDefinitionId(sourceDefinitionId)
              .actorType(io.airbyte.api.model.generated.ActorType.SOURCE));

      final ConnectionReadList connectionReadListForDestinationDefinitionId = connectionsHandler.listConnectionsForActorDefinition(
          new ActorDefinitionRequestBody()
              .actorDefinitionId(destinationDefinitionId)
              .actorType(io.airbyte.api.model.generated.ActorType.DESTINATION));

      assertEquals(
          List.of(ConnectionHelpers.generateExpectedConnectionRead(standardSync)),
          connectionReadListForSourceDefinitionId.getConnections());
      assertEquals(
          List.of(ConnectionHelpers.generateExpectedConnectionRead(standardSync2)),
          connectionReadListForDestinationDefinitionId.getConnections());
    }

    @Test
    void testSearchConnections()
        throws JsonValidationException, ConfigNotFoundException, IOException {
      final ConnectionRead connectionRead1 = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
      final StandardSync standardSync2 = new StandardSync()
          .withConnectionId(UUID.randomUUID())
          .withName("test connection")
          .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
          .withNamespaceFormat("ns_format")
          .withPrefix("test_prefix")
          .withStatus(StandardSync.Status.ACTIVE)
          .withCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog())
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withOperationIds(List.of(operationId))
          .withManual(true)
          .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
          .withGeography(Geography.US)
          .withBreakingChange(false)
          .withNotifySchemaChanges(false)
          .withNotifySchemaChangesByEmail(true);
      final ConnectionRead connectionRead2 = ConnectionHelpers.connectionReadFromStandardSync(standardSync2);
      final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
          .withName(SOURCE_TEST)
          .withSourceDefinitionId(UUID.randomUUID());
      final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
          .withName(DESTINATION_TEST)
          .withDestinationDefinitionId(UUID.randomUUID());
      final ActorDefinitionVersion sourceVersion = mock(ActorDefinitionVersion.class);
      final ActorDefinitionVersion destinationVersion = mock(ActorDefinitionVersion.class);

      when(connectionService.listStandardSyncs())
          .thenReturn(Lists.newArrayList(standardSync, standardSync2));
      when(sourceService.getSourceConnection(source.getSourceId()))
          .thenReturn(source);
      when(destinationService.getDestinationConnection(destination.getDestinationId()))
          .thenReturn(destination);
      when(connectionService.getStandardSync(standardSync.getConnectionId()))
          .thenReturn(standardSync);
      when(connectionService.getStandardSync(standardSync2.getConnectionId()))
          .thenReturn(standardSync2);
      when(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
          .thenReturn(sourceDefinition);
      when(destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId()))
          .thenReturn(destinationDefinition);
      when(actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, source.getWorkspaceId(), source.getSourceId()))
          .thenReturn(new ActorDefinitionVersionWithOverrideStatus(sourceVersion, false));
      when(actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, destination.getWorkspaceId(),
          destination.getDestinationId()))
              .thenReturn(new ActorDefinitionVersionWithOverrideStatus(destinationVersion, false));

      final ConnectionSearch connectionSearch = new ConnectionSearch();
      connectionSearch.namespaceDefinition(NamespaceDefinitionType.SOURCE);
      ConnectionReadList actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));

      connectionSearch.namespaceDefinition(null);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(2, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(1));

      final SourceSearch sourceSearch = new SourceSearch().sourceId(UUID.randomUUID());
      connectionSearch.setSource(sourceSearch);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(0, actualConnectionReadList.getConnections().size());

      sourceSearch.sourceId(connectionRead1.getSourceId());
      connectionSearch.setSource(sourceSearch);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(2, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(1));

      final DestinationSearch destinationSearch = new DestinationSearch();
      connectionSearch.setDestination(destinationSearch);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(2, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(1));

      destinationSearch.connectionConfiguration(Jsons.jsonNode(Collections.singletonMap("apiKey", "not-found")));
      connectionSearch.setDestination(destinationSearch);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(0, actualConnectionReadList.getConnections().size());

      destinationSearch.connectionConfiguration(Jsons.jsonNode(Collections.singletonMap("apiKey", "123-abc")));
      connectionSearch.setDestination(destinationSearch);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(2, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(1));

      connectionSearch.name("non-existent");
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(0, actualConnectionReadList.getConnections().size());

      connectionSearch.name(connectionRead1.getName());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));

      connectionSearch.name(connectionRead2.getName());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(0));

      connectionSearch.namespaceDefinition(connectionRead1.getNamespaceDefinition());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(0, actualConnectionReadList.getConnections().size());

      connectionSearch.name(null);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));

      connectionSearch.namespaceDefinition(connectionRead2.getNamespaceDefinition());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(0));

      connectionSearch.namespaceDefinition(null);
      connectionSearch.status(ConnectionStatus.INACTIVE);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(0, actualConnectionReadList.getConnections().size());

      connectionSearch.status(ConnectionStatus.ACTIVE);
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(2, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(1));

      connectionSearch.prefix(connectionRead1.getPrefix());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead1, actualConnectionReadList.getConnections().get(0));

      connectionSearch.prefix(connectionRead2.getPrefix());
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch);
      assertEquals(1, actualConnectionReadList.getConnections().size());
      assertEquals(connectionRead2, actualConnectionReadList.getConnections().get(0));
    }

    @Test
    void testDeleteConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
      connectionsHandler.deleteConnection(connectionId);

      verify(connectionHelper).deleteConnection(connectionId);
      verify(streamRefreshesHandler).deleteRefreshesForConnection(connectionId);
    }

    @Test
    void failOnUnmatchedWorkspacesInCreate() throws JsonValidationException, ConfigNotFoundException, IOException {
      when(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(standardSync.getSourceId())).thenReturn(UUID.randomUUID());
      when(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(standardSync.getDestinationId())).thenReturn(UUID.randomUUID());
      when(sourceService.getSourceConnection(source.getSourceId()))
          .thenReturn(source);
      when(destinationService.getDestinationConnection(destination.getDestinationId()))
          .thenReturn(destination);

      when(uuidGenerator.get()).thenReturn(standardSync.getConnectionId());
      final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
          .withName(SOURCE_TEST)
          .withSourceDefinitionId(UUID.randomUUID());
      final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
          .withName(DESTINATION_TEST)
          .withDestinationDefinitionId(UUID.randomUUID());
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);
      when(sourceService.getSourceDefinitionFromConnection(standardSync.getConnectionId())).thenReturn(sourceDefinition);
      when(destinationService.getDestinationDefinitionFromConnection(standardSync.getConnectionId())).thenReturn(destinationDefinition);

      final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

      final ConnectionCreate connectionCreate = new ConnectionCreate()
          .sourceId(standardSync.getSourceId())
          .destinationId(standardSync.getDestinationId())
          .operationIds(standardSync.getOperationIds())
          .name(PRESTO_TO_HUDI)
          .namespaceDefinition(NamespaceDefinitionType.SOURCE)
          .namespaceFormat(null)
          .prefix(PRESTO_TO_HUDI_PREFIX)
          .status(ConnectionStatus.ACTIVE)
          .schedule(ConnectionHelpers.generateBasicConnectionSchedule())
          .syncCatalog(catalog)
          .resourceRequirements(new io.airbyte.api.model.generated.ResourceRequirements()
              .cpuRequest(standardSync.getResourceRequirements().getCpuRequest())
              .cpuLimit(standardSync.getResourceRequirements().getCpuLimit())
              .memoryRequest(standardSync.getResourceRequirements().getMemoryRequest())
              .memoryLimit(standardSync.getResourceRequirements().getMemoryLimit()));

      Assert.assertThrows(IllegalArgumentException.class, () -> {
        connectionsHandler.createConnection(connectionCreate);
      });
    }

    @Test
    void testEnumConversion() {
      assertTrue(Enums.isCompatible(ConnectionStatus.class, StandardSync.Status.class));
      assertTrue(Enums.isCompatible(io.airbyte.config.SyncMode.class, SyncMode.class));
      assertTrue(Enums.isCompatible(StandardSync.Status.class, ConnectionStatus.class));
      assertTrue(Enums.isCompatible(ConnectionSchedule.TimeUnitEnum.class, Schedule.TimeUnit.class));
      assertTrue(Enums.isCompatible(io.airbyte.api.model.generated.DataType.class, DataType.class));
      assertTrue(Enums.isCompatible(DataType.class, io.airbyte.api.model.generated.DataType.class));
      assertTrue(Enums.isCompatible(NamespaceDefinitionType.class, io.airbyte.config.JobSyncConfig.NamespaceDefinitionType.class));
    }

    @Nested
    class CreateConnection {

      @BeforeEach
      void setup() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
        // for create calls
        when(workspaceHelper.getWorkspaceForDestinationId(standardSync.getDestinationId())).thenReturn(workspaceId);
        // for update calls
        when(workspaceHelper.getWorkspaceForConnectionId(standardSync.getConnectionId())).thenReturn(workspaceId);
      }

      private ConnectionCreate buildConnectionCreateRequest(final StandardSync standardSync, final AirbyteCatalog catalog) {
        return new ConnectionCreate()
            .sourceId(standardSync.getSourceId())
            .destinationId(standardSync.getDestinationId())
            .operationIds(standardSync.getOperationIds())
            .name(PRESTO_TO_HUDI)
            .namespaceDefinition(NamespaceDefinitionType.SOURCE)
            .namespaceFormat(null)
            .prefix(PRESTO_TO_HUDI_PREFIX)
            .status(ConnectionStatus.ACTIVE)
            .schedule(ConnectionHelpers.generateBasicConnectionSchedule())
            .syncCatalog(catalog)
            .resourceRequirements(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(standardSync.getResourceRequirements().getCpuRequest())
                .cpuLimit(standardSync.getResourceRequirements().getCpuLimit())
                .memoryRequest(standardSync.getResourceRequirements().getMemoryRequest())
                .memoryLimit(standardSync.getResourceRequirements().getMemoryLimit()))
            .sourceCatalogId(standardSync.getSourceCatalogId())
            .geography(apiPojoConverters.toApiGeography(standardSync.getGeography()))
            .notifySchemaChanges(standardSync.getNotifySchemaChanges())
            .notifySchemaChangesByEmail(standardSync.getNotifySchemaChangesByEmail())
            .backfillPreference(Enums.convertTo(standardSync.getBackfillPreference(), SchemaChangeBackfillPreference.class));
      }

      @Test
      void testCreateConnection()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

        // set a defaultGeography on the workspace as EU, but expect connection to be
        // created AUTO because the ConnectionCreate geography takes precedence over the workspace
        // defaultGeography.
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.EU);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog);

        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);

        assertEquals(expectedConnectionRead, actualConnectionRead);

        verify(connectionService).writeStandardSync(standardSync
            .withNotifySchemaChangesByEmail(null));

        // Use new schedule schema, verify that we get the same results.
        connectionCreate
            .schedule(null)
            .scheduleType(ConnectionScheduleType.BASIC)
            .scheduleData(ConnectionHelpers.generateBasicConnectionScheduleData());
        assertEquals(expectedConnectionRead
            .notifySchemaChangesByEmail(null),
            connectionsHandler.createConnection(connectionCreate));
      }

      @Test
      void testCreateConnectionWithDuplicateStreamsShouldThrowException() {

        final AirbyteCatalog catalog = ConnectionHelpers.generateMultipleStreamsApiCatalog(false, false, 2);

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog);

        assertThrows(IllegalArgumentException.class, () -> connectionsHandler.createConnection(connectionCreate));

      }

      @Test
      void testCreateConnectionUsesDefaultGeographyFromWorkspace()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {

        when(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId);

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

        // don't set a geography on the ConnectionCreate to force inheritance from workspace default
        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog).geography(null);

        // set the workspace default to EU
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.EU);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        // the expected read and verified write is generated from the standardSync, so set this to EU as
        // well
        standardSync.setGeography(Geography.EU);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        assertEquals(expectedConnectionRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(standardSync
            .withNotifySchemaChangesByEmail(null));
      }

      @Test
      void testCreateConnectionWithSelectedFields()
          throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.AUTO);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final AirbyteCatalog catalogWithSelectedFields = ConnectionHelpers.generateApiCatalogWithTwoFields();
        // Only select one of the two fields.
        catalogWithSelectedFields.getStreams().get(0).getConfig().fieldSelectionEnabled(true)
            .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)));

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalogWithSelectedFields);

        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);

        assertEquals(expectedConnectionRead, actualConnectionRead);

        standardSync.withFieldSelectionData(new FieldSelectionData().withAdditionalProperty(STREAM_SELECTION_DATA, true));

        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null));
      }

      @Test
      void testCreateConnectionWithHashedFields()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.EU);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
        catalog.getStreams().getFirst().getConfig().hashedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)));

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog);

        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
        assertEquals(expectedConnectionRead, actualConnectionRead);

        standardSync.getCatalog().getStreams().getFirst().setMappers(List.of(MapperHelperKt.createHashingMapper(FIELD_NAME)));
        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null));
      }

      @Test
      void testCreateConnectionWithMappers()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.EU);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final UUID newMapperId = UUID.randomUUID();
        when(uuidGenerator.get()).thenReturn(connectionId, newMapperId);
        final MapperConfig hashingMapper = MapperHelperKt.createHashingMapper(FIELD_NAME, newMapperId);

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
        catalog.getStreams().getFirst().getConfig().mappers(List.of(new ConfiguredStreamMapper()
            .type(StreamMapperType.HASHING)
            .mapperConfiguration(Jsons.jsonNode(hashingMapper.config()))));

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog);

        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
        assertEquals(expectedConnectionRead, actualConnectionRead);

        standardSync.getCatalog().getStreams().getFirst().setMappers(List.of(hashingMapper));
        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null));
      }

      @Test
      void testCreateConnectionValidatesMappers() throws JsonValidationException, ConfigNotFoundException, IOException {
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.EU);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, catalog);

        final String streamName = "stream-name";
        when(destinationCatalogGenerator.generateDestinationCatalog(catalogConverter.toConfiguredInternal(catalog)))
            .thenReturn(new CatalogGenerationResult(new ConfiguredAirbyteCatalog(),
                Map.of(
                    new io.airbyte.config.StreamDescriptor().withName(streamName), Map.of(
                        new MapperConfig() {

                          @NotNull
                          @Override
                          public String name() {
                            return MapperOperationName.HASHING;
                          }

                          @Nullable
                          @Override
                          public UUID id() {
                            return null;
                          }

                          @Nullable
                          @Override
                          public String documentationUrl() {
                            return null;
                          }

                          @NotNull
                          @Override
                          public Object config() {
                            return Map.of();
                          }

                        }, new MapperError(MapperErrorType.INVALID_MAPPER_CONFIG, "error message")))));

        final MapperValidationProblem exception =
            assertThrows(MapperValidationProblem.class, () -> connectionsHandler.createConnection(connectionCreate));
        final MapperValidationProblemResponse problem = (MapperValidationProblemResponse) exception.getProblem();
        assertEquals(problem.getData().getErrors().size(), 1);
        assertEquals(problem.getData().getErrors().getFirst(),
            new ProblemMapperErrorData()
                .stream(streamName)
                .error(MapperErrorType.INVALID_MAPPER_CONFIG.name())
                .mapper(new ProblemMapperErrorDataMapper().type(MapperOperationName.HASHING).mapperConfiguration(Map.of())));
      }

      @Test
      void testCreateFullRefreshConnectionWithSelectedFields()
          throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
        final StandardWorkspace workspace = new StandardWorkspace()
            .withWorkspaceId(workspaceId)
            .withDefaultGeography(Geography.AUTO);
        when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace);

        final AirbyteCatalog fullRefreshCatalogWithSelectedFields = ConnectionHelpers.generateApiCatalogWithTwoFields();
        fullRefreshCatalogWithSelectedFields.getStreams().get(0).getConfig()
            .fieldSelectionEnabled(true)
            .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
            .cursorField(null)
            .syncMode(SyncMode.FULL_REFRESH);

        final ConnectionCreate connectionCreate = buildConnectionCreateRequest(standardSync, fullRefreshCatalogWithSelectedFields);

        final ConnectionRead actualConnectionRead = connectionsHandler.createConnection(connectionCreate);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);

        assertEquals(expectedConnectionRead, actualConnectionRead);

        standardSync
            .withFieldSelectionData(new FieldSelectionData().withAdditionalProperty(STREAM_SELECTION_DATA, true))
            .getCatalog().getStreams().get(0).withSyncMode(io.airbyte.config.SyncMode.FULL_REFRESH).withCursorField(null);

        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null));
      }

      @Test
      void testFieldSelectionRemoveCursorFails() throws JsonValidationException, ConfigNotFoundException, IOException {
        // Test that if we try to de-select a field that's being used for the cursor, the request will fail.
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        // Send an update that sets a cursor but de-selects that field.
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        catalogForUpdate.getStreams().get(0).getConfig()
            .fieldSelectionEnabled(true)
            .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
            .cursorField(List.of(SECOND_FIELD_NAME))
            .syncMode(SyncMode.INCREMENTAL);

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        assertThrows(JsonValidationException.class, () -> connectionsHandler.updateConnection(connectionUpdate, null, false));
      }

      @Test
      void testFieldSelectionRemovePrimaryKeyFails() throws JsonValidationException, ConfigNotFoundException, IOException {
        // Test that if we try to de-select a field that's being used for the primary key, the request will
        // fail.
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        // Send an update that sets a primary key but deselects that field.
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        catalogForUpdate.getStreams().get(0).getConfig()
            .fieldSelectionEnabled(true)
            .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
            .destinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
            .primaryKey(List.of(List.of(SECOND_FIELD_NAME)));

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        assertThrows(JsonValidationException.class, () -> connectionsHandler.updateConnection(connectionUpdate, null, false));
      }

      @Test
      void testValidateConnectionCreateSourceAndDestinationInDifferenceWorkspace() {

        when(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)).thenReturn(UUID.randomUUID());

        final ConnectionCreate connectionCreate = new ConnectionCreate()
            .sourceId(standardSync.getSourceId())
            .destinationId(standardSync.getDestinationId());

        assertThrows(IllegalArgumentException.class, () -> connectionsHandler.createConnection(connectionCreate));
      }

      @Test
      void testValidateConnectionCreateOperationInDifferentWorkspace() {

        when(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(UUID.randomUUID());

        final ConnectionCreate connectionCreate = new ConnectionCreate()
            .sourceId(standardSync.getSourceId())
            .destinationId(standardSync.getDestinationId())
            .operationIds(Collections.singletonList(operationId));

        assertThrows(IllegalArgumentException.class, () -> connectionsHandler.createConnection(connectionCreate));
      }

      @Test
      void testCreateConnectionWithBadDefinitionIds() throws JsonValidationException, ConfigNotFoundException, IOException {

        final UUID sourceIdBad = UUID.randomUUID();
        final UUID destinationIdBad = UUID.randomUUID();

        when(sourceService.getSourceConnection(sourceIdBad))
            .thenThrow(new ConfigNotFoundException(ConfigSchema.SOURCE_CONNECTION, sourceIdBad));
        when(destinationService.getDestinationConnection(destinationIdBad))
            .thenThrow(new ConfigNotFoundException(ConfigSchema.DESTINATION_CONNECTION, destinationIdBad));

        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();

        final ConnectionCreate connectionCreateBadSource = new ConnectionCreate()
            .sourceId(sourceIdBad)
            .destinationId(standardSync.getDestinationId())
            .operationIds(standardSync.getOperationIds())
            .name(PRESTO_TO_HUDI)
            .namespaceDefinition(NamespaceDefinitionType.SOURCE)
            .namespaceFormat(null)
            .prefix(PRESTO_TO_HUDI_PREFIX)
            .status(ConnectionStatus.ACTIVE)
            .schedule(ConnectionHelpers.generateBasicConnectionSchedule())
            .syncCatalog(catalog);

        assertThrows(ConfigNotFoundException.class, () -> connectionsHandler.createConnection(connectionCreateBadSource));

        final ConnectionCreate connectionCreateBadDestination = new ConnectionCreate()
            .sourceId(standardSync.getSourceId())
            .destinationId(destinationIdBad)
            .operationIds(standardSync.getOperationIds())
            .name(PRESTO_TO_HUDI)
            .namespaceDefinition(NamespaceDefinitionType.SOURCE)
            .namespaceFormat(null)
            .prefix(PRESTO_TO_HUDI_PREFIX)
            .status(ConnectionStatus.ACTIVE)
            .schedule(ConnectionHelpers.generateBasicConnectionSchedule())
            .syncCatalog(catalog);

        assertThrows(ConfigNotFoundException.class, () -> connectionsHandler.createConnection(connectionCreateBadDestination));
      }

      @Test
      void throwsBadRequestExceptionOnCatalogSizeValidationError() {
        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
        final ConnectionCreate request = buildConnectionCreateRequest(standardSync, catalog);
        when(catalogValidator.fieldCount(eq(catalog), any())).thenReturn(new ValidationError("bad catalog"));

        assertThrows(BadRequestException.class, () -> connectionsHandler.createConnection(request));
      }

    }

    @Nested
    class UpdateConnection {

      StandardSync moreComplexCatalogSync;

      ConfiguredAirbyteCatalog complexConfiguredCatalog;

      AirbyteCatalog complexCatalog;

      final List<String> catalogStreamNames = List.of("user", "permission", "organization", "workspace", "order");

      @BeforeEach
      void setup() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
        final UUID connection3Id = UUID.randomUUID();
        when(workspaceHelper.getWorkspaceForConnectionId(standardSync.getConnectionId())).thenReturn(workspaceId);

        complexConfiguredCatalog = new ConfiguredAirbyteCatalog()
            .withStreams(catalogStreamNames.stream().map(this::buildConfiguredStream).toList());

        complexCatalog = new AirbyteCatalog().streams(catalogStreamNames.stream().map(this::buildStream).toList());

        moreComplexCatalogSync = new StandardSync()
            .withConnectionId(connection3Id)
            .withName("Connection with non trivial catalog")
            .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
            .withNamespaceFormat(null)
            .withPrefix("none")
            .withStatus(StandardSync.Status.ACTIVE)
            .withCatalog(complexConfiguredCatalog)
            .withSourceId(sourceId)
            .withDestinationId(destinationId)
            .withOperationIds(List.of())
            .withManual(false)
            .withSchedule(ConnectionHelpers.generateBasicSchedule())
            .withScheduleType(ScheduleType.BASIC_SCHEDULE)
            .withScheduleData(ConnectionHelpers.generateBasicScheduleData())
            .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
            .withSourceCatalogId(UUID.randomUUID())
            .withGeography(Geography.AUTO)
            .withNotifySchemaChanges(false)
            .withNotifySchemaChangesByEmail(true)
            .withBreakingChange(false);
        when(connectionService.getStandardSync(moreComplexCatalogSync.getConnectionId())).thenReturn(moreComplexCatalogSync);
        when(workspaceHelper.getWorkspaceForConnectionId(connection3Id)).thenReturn(workspaceId);
        when(sourceService.getSourceDefinitionFromConnection(connection3Id))
            .thenReturn(new StandardSourceDefinition().withName("source").withSourceDefinitionId(UUID.randomUUID()));
        when(destinationService.getDestinationDefinitionFromConnection(connection3Id))
            .thenReturn(new StandardDestinationDefinition().withName("destination").withDestinationDefinitionId(UUID.randomUUID()));
      }

      private ConfiguredAirbyteStream buildConfiguredStream(final String name) {
        return new ConfiguredAirbyteStream(
            CatalogHelpers.createAirbyteStream(name, Field.of(FIELD_NAME, JsonSchemaType.STRING))
                .withDefaultCursorField(List.of(FIELD_NAME))
                .withSourceDefinedCursor(false)
                .withSupportedSyncModes(
                    List.of(io.airbyte.config.SyncMode.FULL_REFRESH, io.airbyte.config.SyncMode.INCREMENTAL)),
            io.airbyte.config.SyncMode.INCREMENTAL,
            io.airbyte.config.DestinationSyncMode.APPEND)
                .withCursorField(List.of(FIELD_NAME));
      }

      private AirbyteStreamAndConfiguration buildStream(final String name) {
        return new AirbyteStreamAndConfiguration()
            .stream(new AirbyteStream().name(name).jsonSchema(Jsons.emptyObject()).supportedSyncModes(List.of(SyncMode.INCREMENTAL)))
            .config(new AirbyteStreamConfiguration().syncMode(SyncMode.INCREMENTAL).destinationSyncMode(DestinationSyncMode.APPEND).selected(true));
      }

      @Test
      void testUpdateConnectionPatchSingleField() throws Exception {
        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .name("newName");

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .name("newName");
        final StandardSync expectedPersistedSync = Jsons.clone(standardSync).withName("newName");

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchScheduleToManual() throws Exception {
        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .scheduleType(ConnectionScheduleType.MANUAL);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .schedule(null)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .scheduleData(null);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withSchedule(null)
            .withScheduleType(ScheduleType.MANUAL)
            .withScheduleData(null)
            .withManual(true);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionWithDuplicateStreamsShouldThrowException() {
        final AirbyteCatalog catalog = ConnectionHelpers.generateMultipleStreamsApiCatalog(false, false, 2);

        final ConnectionUpdate connectionCreate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalog);

        assertThrows(IllegalArgumentException.class, () -> connectionsHandler.updateConnection(connectionCreate, null, false));
      }

      @Test
      void testUpdateConnectionPatchScheduleToCron() throws Exception {
        when(workspaceHelper.getWorkspaceForSourceId(any())).thenReturn(UUID.randomUUID());
        when(workspaceHelper.getOrganizationForWorkspace(any())).thenReturn(UUID.randomUUID());

        final ConnectionScheduleData cronScheduleData = new ConnectionScheduleData().cron(
            new ConnectionScheduleDataCron().cronExpression(CRON_EXPRESSION).cronTimeZone(CRON_TIMEZONE_UTC));

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .scheduleType(ConnectionScheduleType.CRON)
            .scheduleData(cronScheduleData);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .schedule(null)
            .scheduleType(ConnectionScheduleType.CRON)
            .scheduleData(cronScheduleData);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withSchedule(null)
            .withScheduleType(ScheduleType.CRON)
            .withScheduleData(new ScheduleData().withCron(new Cron().withCronExpression(CRON_EXPRESSION).withCronTimeZone(CRON_TIMEZONE_UTC)))
            .withManual(false);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchBasicSchedule() throws Exception {

        final ConnectionScheduleData newScheduleData =
            new ConnectionScheduleData().basicSchedule(new ConnectionScheduleDataBasicSchedule().timeUnit(TimeUnitEnum.DAYS).units(10L));

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .scheduleType(ConnectionScheduleType.BASIC) // update route requires this to be set even if it isn't changing
            .scheduleData(newScheduleData);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .schedule(new ConnectionSchedule().timeUnit(ConnectionSchedule.TimeUnitEnum.DAYS).units(10L)) // still dual-writing to legacy field
            .scheduleType(ConnectionScheduleType.BASIC)
            .scheduleData(newScheduleData);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withSchedule(new Schedule().withTimeUnit(TimeUnit.DAYS).withUnits(10L)) // still dual-writing to legacy field
            .withScheduleType(ScheduleType.BASIC_SCHEDULE)
            .withScheduleData(new ScheduleData().withBasicSchedule(new BasicSchedule().withTimeUnit(BasicSchedule.TimeUnit.DAYS).withUnits(10L)))
            .withManual(false);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchAddingNewStream() throws Exception {
        // the connection initially has a catalog with one stream. this test generates another catalog with
        // one stream, changes that stream's name to something new, and sends both streams in the patch
        // request.
        // the test expects the final result to include both streams.
        final AirbyteCatalog catalogWithNewStream = ConnectionHelpers.generateBasicApiCatalog();
        catalogWithNewStream.getStreams().get(0).getStream().setName(AZKABAN_USERS);
        catalogWithNewStream.getStreams().get(0).getConfig().setAliasName(AZKABAN_USERS);

        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateMultipleStreamsApiCatalog(2);
        catalogForUpdate.getStreams().get(1).getStream().setName(AZKABAN_USERS);
        catalogForUpdate.getStreams().get(1).getConfig().setAliasName(AZKABAN_USERS);

        // expect two streams in the final persisted catalog -- the original unchanged stream, plus the new
        // AZKABAN_USERS stream

        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateMultipleStreamsConfiguredAirbyteCatalog(2);
        expectedPersistedCatalog.getStreams().get(1).getStream().setName(AZKABAN_USERS);

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate));

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchEditExistingStreamWhileAddingNewStream() throws Exception {
        // the connection initially has a catalog with two streams. this test updates the catalog
        // with a sync mode change for one of the initial streams while also adding a brand-new
        // stream. The result should be a catalog with three streams.
        standardSync.setCatalog(ConnectionHelpers.generateMultipleStreamsConfiguredAirbyteCatalog(2));

        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateMultipleStreamsApiCatalog(3);
        catalogForUpdate.getStreams().get(0).getConfig().setSyncMode(SyncMode.FULL_REFRESH);
        catalogForUpdate.getStreams().get(2).getStream().setName(AZKABAN_USERS);
        catalogForUpdate.getStreams().get(2).getConfig().setAliasName(AZKABAN_USERS);

        // expect three streams in the final persisted catalog
        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateMultipleStreamsConfiguredAirbyteCatalog(3);
        expectedPersistedCatalog.getStreams().get(0).withSyncMode(io.airbyte.config.SyncMode.FULL_REFRESH);
        // index 1 is unchanged
        expectedPersistedCatalog.getStreams().get(2).getStream().withName(AZKABAN_USERS);

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate));

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchValidatesMappers() throws Exception {
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        final String streamName = "stream-name";
        final UUID mapperId = UUID.randomUUID();
        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);
        when(destinationCatalogGenerator.generateDestinationCatalog(catalogConverter.toConfiguredInternal(catalogForUpdate)))
            .thenReturn(new CatalogGenerationResult(new ConfiguredAirbyteCatalog(),
                Map.of(
                    new io.airbyte.config.StreamDescriptor().withName(streamName), Map.of(
                        new MapperConfig() {

                          @NotNull
                          @Override
                          public Object config() {
                            return Map.of();
                          }

                          @Nullable
                          @Override
                          public String documentationUrl() {
                            return null;
                          }

                          @Override
                          public UUID id() {
                            return mapperId;
                          }

                          @NotNull
                          @Override
                          public String name() {
                            return MapperOperationName.HASHING;
                          }

                        }, new MapperError(MapperErrorType.INVALID_MAPPER_CONFIG, "error")))));

        final MapperValidationProblem exception =
            assertThrows(MapperValidationProblem.class, () -> connectionsHandler.updateConnection(connectionUpdate, null, false));
        final MapperValidationProblemResponse problem = (MapperValidationProblemResponse) exception.getProblem();
        assertEquals(problem.getData().getErrors().size(), 1);
        assertEquals(problem.getData().getErrors().getFirst(),
            new ProblemMapperErrorData()
                .stream(streamName)
                .error(MapperErrorType.INVALID_MAPPER_CONFIG.name())
                .mapper(new ProblemMapperErrorDataMapper().id(mapperId).type(MapperOperationName.HASHING).mapperConfiguration(Map.of())));
      }

      @Test
      void testUpdateConnectionPatchHashedFields() throws Exception {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        // Send an update that hashes one of the fields
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        catalogForUpdate.getStreams().getFirst().getConfig().hashedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)));

        // Expect mapper in the persisted catalog
        final HashingMapperConfig hashingMapper = MapperHelperKt.createHashingMapper(FIELD_NAME);
        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateAirbyteCatalogWithTwoFields();
        expectedPersistedCatalog.getStreams().getFirst().setMappers(List.of(hashingMapper));

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        // Ensure mappers are populated as well
        final AirbyteCatalog expectedCatalog = Jsons.clone(catalogForUpdate);
        expectedCatalog.getStreams().getFirst().getConfig().addMappersItem(
            new ConfiguredStreamMapper().type(StreamMapperType.HASHING).mapperConfiguration(Jsons.jsonNode(hashingMapper.getConfig())));

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .syncCatalog(expectedCatalog);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withCatalog(expectedPersistedCatalog);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchMappers() throws Exception {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        // Send an update that hashes one of the fields, using mappers
        final HashingMapperConfig hashingMapper = MapperHelperKt.createHashingMapper(FIELD_NAME, UUID.randomUUID());
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        catalogForUpdate.getStreams().getFirst().getConfig().addMappersItem(
            new ConfiguredStreamMapper()
                .id(hashingMapper.id())
                .type(StreamMapperType.HASHING)
                .mapperConfiguration(Jsons.jsonNode(hashingMapper.getConfig())));

        // Expect mapper in the persisted catalog
        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateAirbyteCatalogWithTwoFields();
        expectedPersistedCatalog.getStreams().getFirst().setMappers(List.of(hashingMapper));

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        // Ensure hashedFields is set as well for backwards-compatibility with UI expectations
        final AirbyteCatalog expectedCatalog = Jsons.clone(catalogForUpdate);
        expectedCatalog.getStreams().getFirst().getConfig().addHashedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .syncCatalog(expectedCatalog);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withCatalog(expectedPersistedCatalog);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchColumnSelection() throws Exception {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(ConnectionHelpers.generateAirbyteCatalogWithTwoFields());

        // Send an update that only selects one of the fields.
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateApiCatalogWithTwoFields();
        catalogForUpdate.getStreams().get(0).getConfig().fieldSelectionEnabled(true)
            .selectedFields(List.of(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME)));

        // Expect one column in the final persisted catalog
        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog();

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalogForUpdate);

        final ConnectionRead expectedRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate));

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        assertEquals(expectedRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testUpdateConnectionPatchingSeveralFieldsAndReplaceAStream()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
        final AirbyteCatalog catalogForUpdate = ConnectionHelpers.generateMultipleStreamsApiCatalog(2);

        // deselect the existing stream, and add a new stream called 'azkaban_users'.
        // result that we persist and read after update should be a catalog with a single
        // stream called 'azkaban_users'.
        catalogForUpdate.getStreams().get(0).getConfig().setSelected(false);
        catalogForUpdate.getStreams().get(1).getStream().setName(AZKABAN_USERS);
        catalogForUpdate.getStreams().get(1).getConfig().setAliasName(AZKABAN_USERS);

        final UUID newSourceCatalogId = UUID.randomUUID();

        final ResourceRequirements resourceRequirements = new ResourceRequirements()
            .cpuLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuLimit())
            .cpuRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuRequest())
            .memoryLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryLimit())
            .memoryRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryRequest());

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .status(ConnectionStatus.INACTIVE)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .syncCatalog(catalogForUpdate)
            .resourceRequirements(resourceRequirements)
            .sourceCatalogId(newSourceCatalogId)
            .operationIds(List.of(operationId, otherOperationId))
            .geography(io.airbyte.api.model.generated.Geography.EU);

        final ConfiguredAirbyteCatalog expectedPersistedCatalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog();
        expectedPersistedCatalog.getStreams().get(0).getStream().withName(AZKABAN_USERS);

        final StandardSync expectedPersistedSync = Jsons.clone(standardSync)
            .withStatus(Status.INACTIVE)
            .withScheduleType(ScheduleType.MANUAL)
            .withScheduleData(null)
            .withSchedule(null)
            .withManual(true)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate))
            .withResourceRequirements(apiPojoConverters.resourceRequirementsToInternal(resourceRequirements))
            .withSourceCatalogId(newSourceCatalogId)
            .withOperationIds(List.of(operationId, otherOperationId))
            .withGeography(Geography.EU);

        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionRead actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false);

        final AirbyteCatalog expectedCatalogInRead = ConnectionHelpers.generateBasicApiCatalog();
        expectedCatalogInRead.getStreams().get(0).getStream().setName(AZKABAN_USERS);
        expectedCatalogInRead.getStreams().get(0).getConfig().setAliasName(AZKABAN_USERS);

        final ConnectionRead expectedConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(
            standardSync.getConnectionId(),
            standardSync.getSourceId(),
            standardSync.getDestinationId(),
            standardSync.getOperationIds(),
            newSourceCatalogId,
            apiPojoConverters.toApiGeography(standardSync.getGeography()),
            false,
            standardSync.getNotifySchemaChanges(),
            standardSync.getNotifySchemaChangesByEmail(),
            Enums.convertTo(standardSync.getBackfillPreference(), SchemaChangeBackfillPreference.class))
            .status(ConnectionStatus.INACTIVE)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .scheduleData(null)
            .schedule(null)
            .syncCatalog(expectedCatalogInRead)
            .resourceRequirements(resourceRequirements);

        assertEquals(expectedConnectionRead, actualConnectionRead);
        verify(connectionService).writeStandardSync(expectedPersistedSync);
        verify(eventRunner).update(connectionUpdate.getConnectionId());
      }

      @Test
      void testValidateConnectionUpdateOperationInDifferentWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
        when(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(UUID.randomUUID());
        when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

        final ConnectionUpdate connectionUpdate = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .operationIds(Collections.singletonList(operationId))
            .syncCatalog(catalogConverter.toApi(standardSync.getCatalog(), standardSync.getFieldSelectionData()));

        assertThrows(IllegalArgumentException.class, () -> connectionsHandler.updateConnection(connectionUpdate, null, false));
      }

      @Test
      void throwsBadRequestExceptionOnCatalogSizeValidationError() {
        final AirbyteCatalog catalog = ConnectionHelpers.generateBasicApiCatalog();
        final ConnectionUpdate request = new ConnectionUpdate()
            .connectionId(standardSync.getConnectionId())
            .syncCatalog(catalog)
            .name("newName");
        when(catalogValidator.fieldCount(eq(catalog), any())).thenReturn(new ValidationError("bad catalog"));

        assertThrows(BadRequestException.class, () -> connectionsHandler.updateConnection(request, null, false));
      }

      @Test
      void testDeactivateStreamsWipeState()
          throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
        final AirbyteCatalog catalog = complexCatalog;
        final List<String> deactivatedStreams = List.of("user", "permission");
        final List<String> stillActiveStreams = catalogStreamNames.stream().filter(s -> !deactivatedStreams.contains(s)).toList();

        catalog.setStreams(Stream.concat(
            stillActiveStreams.stream().map(this::buildStream),
            deactivatedStreams.stream().map(this::buildStream)
                .peek(s -> s.setConfig(
                    new AirbyteStreamConfiguration().syncMode(SyncMode.INCREMENTAL).destinationSyncMode(DestinationSyncMode.APPEND).selected(false))))
            .toList());
        final ConnectionUpdate request = new ConnectionUpdate()
            .connectionId(moreComplexCatalogSync.getConnectionId())
            .syncCatalog(catalog);
        when(featureFlagClient.boolVariation(ResetStreamsStateWhenDisabled.INSTANCE, new Workspace(workspaceId))).thenReturn(true);
        connectionsHandler.updateConnection(request, null, false);
        final Set<io.airbyte.config.StreamDescriptor> expectedStreams =
            Set.of(new io.airbyte.config.StreamDescriptor().withName("user"), new io.airbyte.config.StreamDescriptor().withName("permission"));
        verify(statePersistence).bulkDelete(moreComplexCatalogSync.getConnectionId(), expectedStreams);
      }

    }

  }

  @Nested
  class ConnectionHistory {

    @BeforeEach
    void setUp() {
      connectionsHandler = new ConnectionsHandler(
          streamRefreshesHandler,
          jobPersistence,
          catalogService,
          uuidGenerator,
          workspaceHelper,
          trackingClient,
          eventRunner,
          connectionHelper,
          featureFlagClient,
          actorDefinitionVersionHelper,
          connectorDefinitionSpecificationHandler,
          streamGenerationRepository,
          catalogGenerationSetter,
          catalogValidator,
          notificationHelper,
          streamStatusesService,
          connectionTimelineEventService,
          connectionTimelineEventHelper,
          statePersistence,
          sourceService,
          destinationService,
          connectionService,
          workspaceService,
          destinationCatalogGenerator, catalogConverter, applySchemaChangeHelper,
          apiPojoConverters, connectionSchedulerHelper, mapperSecretHelper);
    }

    private Attempt generateMockAttemptWithStreamStats(final Instant attemptTime, final List<Map<List<String>, Long>> streamsToRecordsSynced) {
      final List<StreamSyncStats> streamSyncStatsList = streamsToRecordsSynced.stream().map(streamToRecordsSynced -> {
        final List<String> streamKey = new ArrayList<>(streamToRecordsSynced.keySet().iterator().next());
        final String streamNamespace = streamKey.get(0);
        final String streamName = streamKey.get(1);
        final long recordsSynced = streamToRecordsSynced.get(streamKey);

        return new StreamSyncStats()
            .withStreamName(streamName)
            .withStreamNamespace(streamNamespace)
            .withStats(new SyncStats().withRecordsCommitted(recordsSynced));
      }).collect(Collectors.toList());

      final StandardSyncSummary standardSyncSummary = new StandardSyncSummary().withStreamStats(streamSyncStatsList);
      final StandardSyncOutput standardSyncOutput = new StandardSyncOutput().withStandardSyncSummary(standardSyncSummary);
      final JobOutput jobOutput = new JobOutput().withOutputType(OutputType.SYNC).withSync(standardSyncOutput);

      return new Attempt(0, 0, null, null, jobOutput, AttemptStatus.FAILED, null, null, 0, 0, attemptTime.getEpochSecond());
    }

    private Job generateMockJob(final UUID connectionId, final Attempt attempt) {
      return new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.RUNNING, 1001L, 1000L, 1002L);
    }

    @Nested
    class GetConnectionDataHistory {

      @Test
      void testGetConnectionDataHistory() throws IOException {
        final var connectionId = UUID.randomUUID();
        final var numJobs = 10;
        final ConnectionDataHistoryRequestBody apiReq = new ConnectionDataHistoryRequestBody().numberOfJobs(numJobs).connectionId(connectionId);
        final long jobOneId = 1L;
        final long jobTwoId = 2L;

        final Job jobOne = new Job(1, ConfigType.SYNC, connectionId.toString(), null, List.of(), JobStatus.SUCCEEDED, 0L, 0L, 0L);
        final Job jobTwo = new Job(2, ConfigType.REFRESH, connectionId.toString(), null, List.of(), JobStatus.FAILED, 0L, 0L, 0L);

        when(jobPersistence.listJobs(
            Job.SYNC_REPLICATION_TYPES,
            Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED),
            apiReq.getConnectionId().toString(),
            apiReq.getNumberOfJobs())).thenReturn(List.of(jobOne, jobTwo));

        final long jobOneBytesCommitted = 12345L;
        final long jobOneBytesEmitted = 23456L;
        final long jobOneRecordsCommitted = 19L;
        final long jobOneRecordsEmmitted = 20L;
        final long jobOneCreatedAt = 1000L;
        final long jobOneUpdatedAt = 2000L;
        final long jobTwoCreatedAt = 3000L;
        final long jobTwoUpdatedAt = 4000L;
        final long jobTwoBytesCommitted = 98765L;
        final long jobTwoBytesEmmitted = 87654L;
        final long jobTwoRecordsCommitted = 50L;
        final long jobTwoRecordsEmittted = 60L;
        try (final MockedStatic<StatsAggregationHelper> mockStatsAggregationHelper = Mockito.mockStatic(StatsAggregationHelper.class)) {
          mockStatsAggregationHelper.when(() -> StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(Mockito.any(), Mockito.any()))
              .thenReturn(Map.of(
                  jobOneId, new JobWithAttemptsRead().job(
                      new JobRead().createdAt(jobOneCreatedAt).updatedAt(jobOneUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                          new JobAggregatedStats()
                              .bytesCommitted(jobOneBytesCommitted)
                              .bytesEmitted(jobOneBytesEmitted)
                              .recordsCommitted(jobOneRecordsCommitted)
                              .recordsEmitted(jobOneRecordsEmmitted))),
                  jobTwoId, new JobWithAttemptsRead().job(
                      new JobRead().createdAt(jobTwoCreatedAt).updatedAt(jobTwoUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                          new JobAggregatedStats()
                              .bytesCommitted(jobTwoBytesCommitted)
                              .bytesEmitted(jobTwoBytesEmmitted)
                              .recordsCommitted(jobTwoRecordsCommitted)
                              .recordsEmitted(jobTwoRecordsEmittted)))));

          final List<JobSyncResultRead> expected = List.of(
              new JobSyncResultRead()
                  .configType(JobConfigType.SYNC)
                  .jobId(jobOneId)
                  .bytesCommitted(jobOneBytesCommitted)
                  .bytesEmitted(jobOneBytesEmitted)
                  .recordsCommitted(jobOneRecordsCommitted)
                  .recordsEmitted(jobOneRecordsEmmitted)
                  .jobCreatedAt(jobOneCreatedAt)
                  .jobUpdatedAt(jobOneUpdatedAt),
              new JobSyncResultRead()
                  .configType(JobConfigType.SYNC)
                  .jobId(jobTwoId)
                  .bytesCommitted(jobTwoBytesCommitted)
                  .bytesEmitted(jobTwoBytesEmmitted)
                  .recordsCommitted(jobTwoRecordsCommitted)
                  .recordsEmitted(jobTwoRecordsEmittted)
                  .jobCreatedAt(jobTwoCreatedAt)
                  .jobUpdatedAt(jobTwoUpdatedAt));

          assertEquals(expected, connectionsHandler.getConnectionDataHistory(apiReq));
        }
      }

    }

    @Nested
    class GetConnectionStreamHistory {

      @Test
      @DisplayName("Handles empty history response")
      void testStreamHistoryWithEmptyResponse() throws IOException {
        final UUID connectionId = UUID.randomUUID();
        final ConnectionStreamHistoryRequestBody requestBody = new ConnectionStreamHistoryRequestBody()
            .connectionId(connectionId)
            .timezone(TIMEZONE_LOS_ANGELES);

        when(jobPersistence.listAttemptsForConnectionAfterTimestamp(eq(connectionId), eq(ConfigType.SYNC), any(Instant.class)))
            .thenReturn(Collections.emptyList());

        final List<ConnectionStreamHistoryReadItem> actual = connectionsHandler.getConnectionStreamHistory(requestBody);

        final List<ConnectionStreamHistoryReadItem> expected = Collections.emptyList();

        assertEquals(expected, actual);
      }

      @Test
      @DisplayName("Aggregates data correctly")
      void testStreamHistoryAggregation() throws IOException {
        final UUID connectionId = UUID.randomUUID();
        final Instant endTime = Instant.now();
        final Instant startTime = endTime.minus(30, ChronoUnit.DAYS);
        final long attempt1Records = 100L;
        final long attempt2Records = 150L;
        final long attempt3Records = 200L;
        final long attempt4Records = 125L;
        final String streamName = "testStream";
        final String streamNamespace = "testNamespace";
        final String streamName2 = "testStream2";

        // First Attempt - Day 1
        final Attempt attempt1 = generateMockAttemptWithStreamStats(startTime.plus(1, ChronoUnit.DAYS),
            List.of(Map.of(List.of(streamNamespace, streamName),
                attempt1Records))); // 100 records
        final AttemptWithJobInfo attemptWithJobInfo1 = new AttemptWithJobInfo(attempt1, generateMockJob(connectionId, attempt1));

        // Second Attempt - Same Day as First, same stream as first
        final Attempt attempt2 = generateMockAttemptWithStreamStats(startTime.plus(1, ChronoUnit.DAYS),
            List.of(Map.of(List.of(streamNamespace, streamName),
                attempt2Records))); // 100 records
        final AttemptWithJobInfo attemptWithJobInfo2 = new AttemptWithJobInfo(attempt2, generateMockJob(connectionId, attempt2));

        // Third Attempt - Same Day, different stream
        final Attempt attempt3 = generateMockAttemptWithStreamStats(startTime.plus(1, ChronoUnit.DAYS),
            List.of(Map.of(List.of(streamNamespace, streamName2),
                attempt3Records))); // 100 records
        final AttemptWithJobInfo attemptWithJobInfo3 = new AttemptWithJobInfo(attempt3, generateMockJob(connectionId, attempt3));

        // Fourth Attempt - Different day, first stream
        final Attempt attempt4 = generateMockAttemptWithStreamStats(startTime.plus(2, ChronoUnit.DAYS),
            List.of(Map.of(List.of(streamNamespace, streamName),
                attempt4Records))); // 100 records
        final AttemptWithJobInfo attemptWithJobInfo4 = new AttemptWithJobInfo(attempt4, generateMockJob(connectionId, attempt4));

        final List<AttemptWithJobInfo> attempts = Arrays.asList(attemptWithJobInfo1, attemptWithJobInfo2, attemptWithJobInfo3, attemptWithJobInfo4);

        when(jobPersistence.listAttemptsForConnectionAfterTimestamp(eq(connectionId), eq(ConfigType.SYNC), any(Instant.class)))
            .thenReturn(attempts);

        final ConnectionStreamHistoryRequestBody requestBody = new ConnectionStreamHistoryRequestBody()
            .connectionId(connectionId)
            .timezone(TIMEZONE_LOS_ANGELES);
        final List<ConnectionStreamHistoryReadItem> actual = connectionsHandler.getConnectionStreamHistory(requestBody);

        final List<ConnectionStreamHistoryReadItem> expected = new ArrayList<>();
        // expect the first entry to contain stream 1, day 1, 250 records... next item should be stream 2,
        // day 1, 200 records, and final entry should be stream 1, day 2, 125 records
        expected.add(new ConnectionStreamHistoryReadItem()
            .timestamp(Math.toIntExact(startTime.plus(1, ChronoUnit.DAYS)
                .atZone(ZoneId.of(requestBody.getTimezone()))
                .toLocalDate().atStartOfDay(ZoneId.of(requestBody.getTimezone()))
                .toEpochSecond()))
            .streamName(streamName)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt1Records + attempt2Records));
        expected.add(new ConnectionStreamHistoryReadItem()
            .timestamp(Math.toIntExact(startTime.plus(1, ChronoUnit.DAYS)
                .atZone(ZoneId.of(requestBody.getTimezone()))
                .toLocalDate().atStartOfDay(ZoneId.of(requestBody.getTimezone()))
                .toEpochSecond()))
            .streamName(streamName2)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt3Records));
        expected.add(new ConnectionStreamHistoryReadItem()
            .timestamp(Math.toIntExact(startTime.plus(2, ChronoUnit.DAYS)
                .atZone(ZoneId.of(requestBody.getTimezone()))
                .toLocalDate().atStartOfDay(ZoneId.of(requestBody.getTimezone()))
                .toEpochSecond()))
            .streamName(streamName)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt4Records));

        assertEquals(actual, expected);
      }

    }

  }

  @Nested
  class StreamConfigurationDiff {

    @BeforeEach
    void setUp() {
      connectionsHandler = new ConnectionsHandler(
          streamRefreshesHandler,
          jobPersistence,
          catalogService,
          uuidGenerator,
          workspaceHelper,
          trackingClient,
          eventRunner,
          connectionHelper,
          featureFlagClient,
          actorDefinitionVersionHelper,
          connectorDefinitionSpecificationHandler,
          streamGenerationRepository,
          catalogGenerationSetter,
          catalogValidator,
          notificationHelper,
          streamStatusesService,
          connectionTimelineEventService,
          connectionTimelineEventHelper,
          statePersistence,
          sourceService,
          destinationService,
          connectionService,
          workspaceService,
          destinationCatalogGenerator,
          catalogConverter, applySchemaChangeHelper, apiPojoConverters, connectionSchedulerHelper, mapperSecretHelper);
    }

    @Test
    void testNoDiff() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty());
    }

    @Test
    void testNoDiffIfStreamAdded() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty());
    }

    @Test
    void testCursorOrderDoesMatter() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1, "anotherCursor"),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1WithOtherCursorOrder = getStreamConfiguration(
          List.of("anotherCursor", CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1WithOtherCursorOrder),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(1, changedSd.size());
      assertEquals(Set.of(new StreamDescriptor().name(STREAM1)), changedSd);
    }

    @Test
    void testPkOrderDoesntMatter() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1, PK3)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1WithOtherPkOrder = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK3, PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2), List.of(PK3)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteStreamConfiguration streamConfiguration2WithOtherPkOrder = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK3), List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1WithOtherPkOrder),
                  getStreamAndConfig(STREAM2, streamConfiguration2WithOtherPkOrder)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(1, changedSd.size());
      assertEquals(Set.of(new StreamDescriptor().name(STREAM1)), changedSd);
    }

    @Test
    void testNoDiffIfStreamRemove() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1)));

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty());
    }

    @Test
    void testDiffDifferentCursor() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1CursorDiff = getStreamConfiguration(
          List.of(CURSOR1, "anotherCursor"),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1CursorDiff),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(1, changedSd.size());
      assertEquals(Set.of(new StreamDescriptor().name(STREAM1)), changedSd);
    }

    @Test
    void testDiffIfDifferentPrimaryKey() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1WithPkDiff = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1, PK3)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2WithPkDiff = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1), List.of(PK3)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1WithPkDiff),
                  getStreamAndConfig(STREAM2, streamConfiguration2WithPkDiff)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(2, changedSd.size());
      Assertions.assertThat(changedSd)
          .containsExactlyInAnyOrder(new StreamDescriptor().name(STREAM1), new StreamDescriptor().name(STREAM2));
    }

    @Test
    void testDiffDifferentSyncMode() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1CursorDiff = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1CursorDiff),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(1, changedSd.size());
      assertEquals(Set.of(new StreamDescriptor().name(STREAM1)), changedSd);
    }

    @Test
    void testGetCatalogDiffHandlesInvalidTypes() throws IOException, JsonValidationException {
      final String bad_catalog_json = new String(ConnectionsHandlerTest.class.getClassLoader()
          .getResourceAsStream("catalogs/catalog_with_invalid_type.json").readAllBytes(), StandardCharsets.UTF_8);

      final String good_catalog_json = new String(ConnectionsHandlerTest.class.getClassLoader()
          .getResourceAsStream("catalogs/catalog_with_valid_type.json").readAllBytes(), StandardCharsets.UTF_8);

      final String good_catalog_altered_json = new String(ConnectionsHandlerTest.class.getClassLoader()
          .getResourceAsStream("catalogs/catalog_with_valid_type_and_update.json").readAllBytes(), StandardCharsets.UTF_8);

      final String bad_config_catalog_json = new String(ConnectionsHandlerTest.class.getClassLoader()
          .getResourceAsStream("catalogs/configured_catalog_with_invalid_type.json").readAllBytes(), StandardCharsets.UTF_8);

      final String good_config_catalog_json = new String(ConnectionsHandlerTest.class.getClassLoader()
          .getResourceAsStream("catalogs/configured_catalog_with_valid_type.json").readAllBytes(), StandardCharsets.UTF_8);

      // use Jsons.object to convert the json string to an AirbyteCatalog model

      final io.airbyte.protocol.models.AirbyteCatalog badCatalog = Jsons.deserialize(
          bad_catalog_json, io.airbyte.protocol.models.AirbyteCatalog.class);
      final io.airbyte.protocol.models.AirbyteCatalog goodCatalog = Jsons.deserialize(
          good_catalog_json, io.airbyte.protocol.models.AirbyteCatalog.class);
      final io.airbyte.protocol.models.AirbyteCatalog goodCatalogAltered = Jsons.deserialize(
          good_catalog_altered_json, io.airbyte.protocol.models.AirbyteCatalog.class);
      final ConfiguredAirbyteCatalog badConfiguredCatalog = Jsons.deserialize(
          bad_config_catalog_json, io.airbyte.config.ConfiguredAirbyteCatalog.class);
      final ConfiguredAirbyteCatalog goodConfiguredCatalog = Jsons.deserialize(
          good_config_catalog_json, io.airbyte.config.ConfiguredAirbyteCatalog.class);

      // convert the AirbyteCatalog model to the AirbyteCatalog API model

      final AirbyteCatalog convertedGoodCatalog = catalogConverter.toApi(goodCatalog, null);
      final AirbyteCatalog convertedGoodCatalogAltered = catalogConverter.toApi(goodCatalogAltered, null);
      final AirbyteCatalog convertedBadCatalog = catalogConverter.toApi(badCatalog, null);

      // No issue for valid catalogs

      final CatalogDiff gggDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalog, goodConfiguredCatalog, connectionId);
      assertEquals(gggDiff.getTransforms().size(), 0);
      final CatalogDiff ggagDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalogAltered, goodConfiguredCatalog, connectionId);
      assertEquals(ggagDiff.getTransforms().size(), 1);

      // No issue for good catalog and a bad configured catalog

      final CatalogDiff ggbDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalog, badConfiguredCatalog, connectionId);
      assertEquals(ggbDiff.getTransforms().size(), 0);
      final CatalogDiff ggabDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalogAltered, badConfiguredCatalog, connectionId);
      assertEquals(ggabDiff.getTransforms().size(), 1);

      // assert no issue when migrating two or from a catalog with a skippable (slightly invalid) type

      final CatalogDiff bggDiff = connectionsHandler.getDiff(convertedBadCatalog, convertedGoodCatalog, goodConfiguredCatalog, connectionId);
      assertEquals(bggDiff.getTransforms().size(), 1);

      final CatalogDiff gbgDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedBadCatalog, goodConfiguredCatalog, connectionId);
      assertEquals(gbgDiff.getTransforms().size(), 1);
    }

    @Test
    void testDiffDifferentDestinationSyncMode() {
      final AirbyteStreamConfiguration streamConfiguration1 = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null);

      final AirbyteStreamConfiguration streamConfiguration1CursorDiff = getStreamConfiguration(
          List.of(CURSOR1),
          List.of(List.of(PK1)),
          SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
          null);

      final AirbyteStreamConfiguration streamConfiguration2 = getStreamConfiguration(
          List.of(CURSOR2),
          List.of(List.of(PK2)),
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null);

      final AirbyteCatalog catalog1 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));
      final AirbyteCatalog catalog2 = new AirbyteCatalog()
          .streams(
              List.of(
                  getStreamAndConfig(STREAM1, streamConfiguration1CursorDiff),
                  getStreamAndConfig(STREAM2, streamConfiguration2)));

      final Set<StreamDescriptor> changedSd = connectionsHandler.getConfigurationDiff(catalog1, catalog2);
      assertFalse(changedSd.isEmpty());
      assertEquals(1, changedSd.size());
      assertEquals(Set.of(new StreamDescriptor().name(STREAM1)), changedSd);
    }

    @Test
    void testConnectionStatus() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final AttemptFailureSummary failureSummary = new AttemptFailureSummary();
      failureSummary.setFailures(List.of(new FailureReason().withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)));
      final Attempt failedAttempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, failureSummary, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.RUNNING, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(failedAttempt), JobStatus.FAILED, 901L, 900L, 902L),
          new Job(2L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      assertEquals(1, status.size());

      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(connectionId, connectionStatus.getConnectionId());
      assertEquals(802L, connectionStatus.getLastSuccessfulSync());
      assertEquals(0L, connectionStatus.getActiveJob().getId());
    }

    @Test
    void testConnectionStatus_syncing() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.RUNNING, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.RUNNING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_failed_breakingSchemaChange() throws IOException, JsonValidationException, ConfigNotFoundException {
      final StandardSync standardSyncWithBreakingSchemaChange = Jsons.clone(standardSync).withBreakingChange(true);
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSyncWithBreakingSchemaChange);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.FAILED, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_failed_hasConfigError() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final AttemptFailureSummary failureSummary = new AttemptFailureSummary();
      failureSummary.setFailures(List.of(new FailureReason().withFailureType(FailureReason.FailureType.CONFIG_ERROR)
          .withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)));
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, failureSummary, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.FAILED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.FAILED, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_paused_inactive() throws IOException, JsonValidationException, ConfigNotFoundException {
      final StandardSync standardSyncPaused = Jsons.clone(standardSync).withStatus(Status.INACTIVE);
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSyncPaused);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PAUSED, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_paused_deprecated() throws IOException, JsonValidationException, ConfigNotFoundException {
      final StandardSync standardSyncPaused = Jsons.clone(standardSync).withStatus(Status.DEPRECATED);
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSyncPaused);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PAUSED, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_pending_nosyncs() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final List<Job> jobs = List.of();
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PENDING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_pending_afterSuccessfulReset() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.RESET_CONNECTION, connectionId.toString(), null, List.of(attempt),
              JobStatus.SUCCEEDED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PENDING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_pending_afterFailedReset() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.RESET_CONNECTION, connectionId.toString(), null, List.of(attempt),
              JobStatus.FAILED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PENDING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_pending_afterSuccessfulClear() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.CLEAR, connectionId.toString(), null, List.of(attempt),
              JobStatus.SUCCEEDED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PENDING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_pending_afterFailedClear() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.CLEAR, connectionId.toString(), null, List.of(attempt),
              JobStatus.FAILED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.PENDING, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_incomplete_afterCancelledReset() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt resetAttempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L);
      final Attempt successAttempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.RESET_CONNECTION, connectionId.toString(), null, List.of(resetAttempt),
              JobStatus.CANCELLED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(successAttempt), JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.INCOMPLETE, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_incomplete_failed() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.FAILED, 1001L, 1000L, 1002L),
          new Job(1L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, null, JobStatus.SUCCEEDED, 801L, 800L, 802L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.INCOMPLETE, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_incomplete_cancelled() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt failedAttempt = new Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(failedAttempt),
              JobStatus.CANCELLED, 1001L, 1000L, 1002L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.INCOMPLETE, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    @Test
    void testConnectionStatus_synced() throws IOException, JsonValidationException, ConfigNotFoundException {
      when(connectionService.getStandardSync(standardSync.getConnectionId())).thenReturn(standardSync);

      final UUID connectionId = standardSync.getConnectionId();
      final Attempt attempt = new Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L);
      final List<Job> jobs = List.of(
          new Job(0L, JobConfig.ConfigType.SYNC, connectionId.toString(), null, List.of(attempt), JobStatus.SUCCEEDED, 1001L, 1000L, 1002L));
      when(jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(), 10))
              .thenReturn(jobs);
      final ConnectionStatusesRequestBody req = new ConnectionStatusesRequestBody().connectionIds(List.of(connectionId));
      final List<ConnectionStatusRead> status = connectionsHandler.getConnectionStatuses(req);
      final ConnectionStatusRead connectionStatus = status.get(0);
      assertEquals(Enums.convertTo(ConnectionSyncStatus.SYNCED, io.airbyte.api.model.generated.ConnectionSyncStatus.class),
          connectionStatus.getConnectionSyncStatus());
    }

    private AirbyteStreamAndConfiguration getStreamAndConfig(final String name, final AirbyteStreamConfiguration config) {
      return new AirbyteStreamAndConfiguration()
          .config(config)
          .stream(new AirbyteStream().name(name));
    }

    private AirbyteStreamConfiguration getStreamConfiguration(final List<String> cursors,
                                                              final List<List<String>> primaryKeys,
                                                              final SyncMode syncMode,
                                                              final DestinationSyncMode destinationSyncMode,
                                                              final List<SelectedFieldInfo> hashedFields) {
      return new AirbyteStreamConfiguration()
          .cursorField(cursors)
          .primaryKey(primaryKeys)
          .syncMode(syncMode)
          .destinationSyncMode(destinationSyncMode)
          .hashedFields(hashedFields);

    }

  }

  /**
   * Tests for the applySchemaChanges endpoint. Note that most of the core auto-propagation logic is
   * tested directly on AutoPropagateSchemaChangeHelper.getUpdatedSchema().
   */
  @Nested
  class ApplySchemaChanges {

    private static final String SOURCE_PROTOCOL_VERSION = "0.4.5";
    private static final String SHOES = "shoes";
    private static final String SKU = "sku";
    private static final String A_DIFFERENT_STREAM = "a-different-stream";
    private static final ActorDefinitionVersion SOURCE_VERSION = new ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION);

    private static final UUID SOURCE_CATALOG_ID = UUID.randomUUID();
    private static final UUID CONNECTION_ID = UUID.randomUUID();
    private static final UUID SOURCE_ID = UUID.randomUUID();
    private static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
    private static final UUID WORKSPACE_ID = UUID.randomUUID();
    private static final UUID DESTINATION_ID = UUID.randomUUID();
    private static final UUID DISCOVERED_CATALOG_ID = UUID.randomUUID();
    private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());
    private static final io.airbyte.protocol.models.AirbyteCatalog airbyteCatalog =
        io.airbyte.protocol.models.CatalogHelpers.createAirbyteCatalog(SHOES, Field.of(SKU, JsonSchemaType.STRING));
    private static final ConfiguredAirbyteCatalog configuredAirbyteCatalog =
        catalogHelpers.createConfiguredAirbyteCatalog(SHOES, null, Field.of(SKU, JsonSchemaType.STRING));
    private static final String A_DIFFERENT_NAMESPACE = "a-different-namespace";
    private static final String A_DIFFERENT_COLUMN = "a-different-column";
    private static StandardSync standardSync;
    private static final NotificationSettings NOTIFICATION_SETTINGS = new NotificationSettings();
    private static final String EMAIL = "WorkspaceEmail@Company.com";
    private static final StandardWorkspace WORKSPACE = new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withEmail(EMAIL)
        .withNotificationSettings(NOTIFICATION_SETTINGS);
    private static final ActorCatalog actorCatalog = new ActorCatalog()
        .withCatalog(Jsons.jsonNode(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID());

    @BeforeEach
    void setup() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException,
        io.airbyte.config.persistence.ConfigNotFoundException {
      airbyteCatalog.getStreams().get(0).withSupportedSyncModes(List.of(io.airbyte.protocol.models.SyncMode.FULL_REFRESH));
      standardSync = new StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(configuredAirbyteCatalog)
          .withManual(true)
          .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY);

      when(catalogService.getActorCatalogById(SOURCE_CATALOG_ID)).thenReturn(actorCatalog);
      when(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(standardSync);
      when(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(source);
      when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)).thenReturn(WORKSPACE);
      when(destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID)).thenReturn(
          new StandardDestinationDefinition().withDestinationDefinitionId(DESTINATION_DEFINITION_ID));
      when(connectorDefinitionSpecificationHandler.getDestinationSpecification(new DestinationDefinitionIdWithWorkspaceId()
          .workspaceId(WORKSPACE_ID).destinationDefinitionId(DESTINATION_DEFINITION_ID)))
              .thenReturn(new DestinationDefinitionSpecificationRead().supportedDestinationSyncModes(List.of(DestinationSyncMode.OVERWRITE)));
      when(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID)).thenReturn(WORKSPACE_ID);
      when(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DESTINATION_ID)).thenReturn(WORKSPACE_ID);
      when(workspaceHelper.getWorkspaceForConnectionId(CONNECTION_ID)).thenReturn(WORKSPACE_ID);
      connectionsHandler = new ConnectionsHandler(
          streamRefreshesHandler,
          jobPersistence,
          catalogService,
          uuidGenerator,
          workspaceHelper,
          trackingClient,
          eventRunner,
          connectionHelper,
          featureFlagClient,
          actorDefinitionVersionHelper,
          connectorDefinitionSpecificationHandler,
          streamGenerationRepository,
          catalogGenerationSetter,
          catalogValidator,
          notificationHelper,
          streamStatusesService,
          connectionTimelineEventService,
          connectionTimelineEventHelper,
          statePersistence,
          sourceService,
          destinationService,
          connectionService,
          workspaceService,
          destinationCatalogGenerator,
          catalogConverter, applySchemaChangeHelper,
          apiPojoConverters, connectionSchedulerHelper, mapperSecretHelper);
    }

    @Test
    void testAutoPropagateSchemaChange()
        throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException,
        io.airbyte.config.persistence.ConfigNotFoundException {
      // Somehow standardSync is being mutated in the test (the catalog is changed) and verifying that the
      // notification function is called correctly requires the original object.
      final StandardSync originalSync = Jsons.clone(standardSync);
      final io.airbyte.api.model.generated.AirbyteCatalog catalogWithDiff =
          catalogConverter.toApi(Jsons.clone(airbyteCatalog), SOURCE_VERSION);
      catalogWithDiff.addStreamsItem(new AirbyteStreamAndConfiguration()
          .stream(new AirbyteStream().name(A_DIFFERENT_STREAM).namespace(A_DIFFERENT_NAMESPACE).sourceDefinedCursor(false)
              .jsonSchema(Jsons.emptyObject()).supportedSyncModes(List.of(SyncMode.FULL_REFRESH)))
          .config(
              new AirbyteStreamConfiguration().syncMode(SyncMode.FULL_REFRESH).destinationSyncMode(DestinationSyncMode.OVERWRITE).selected(true)));

      final ConnectionAutoPropagateSchemaChange request = new ConnectionAutoPropagateSchemaChange()
          .connectionId(CONNECTION_ID)
          .workspaceId(WORKSPACE_ID)
          .catalogId(SOURCE_CATALOG_ID)
          .catalog(catalogWithDiff);

      final ConnectionAutoPropagateResult actualResult = connectionsHandler.applySchemaChange(request);

      final CatalogDiff expectedDiff =
          new CatalogDiff().addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
              .streamDescriptor(new StreamDescriptor().namespace(A_DIFFERENT_NAMESPACE).name(A_DIFFERENT_STREAM)));
      assertEquals(expectedDiff, actualResult.getPropagatedDiff());
      final ConfiguredAirbyteCatalog expectedCatalog = Jsons.clone(configuredAirbyteCatalog);
      expectedCatalog.getStreams().forEach(s -> s.getStream().withSourceDefinedCursor(false));
      expectedCatalog.getStreams()
          .add(new ConfiguredAirbyteStream.Builder()
              .stream(new io.airbyte.config.AirbyteStream(A_DIFFERENT_STREAM, Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH))
                  .withNamespace(A_DIFFERENT_NAMESPACE)
                  .withSourceDefinedCursor(false)
                  .withDefaultCursorField(List.of()))
              .syncMode(io.airbyte.config.SyncMode.FULL_REFRESH)
              .destinationSyncMode(io.airbyte.config.DestinationSyncMode.OVERWRITE)
              .cursorField(List.of())
              .fields(List.of()).build());
      final ArgumentCaptor<StandardSync> standardSyncArgumentCaptor = ArgumentCaptor.forClass(StandardSync.class);
      verify(connectionService).writeStandardSync(standardSyncArgumentCaptor.capture());
      final StandardSync actualStandardSync = standardSyncArgumentCaptor.getValue();
      assertEquals(Jsons.clone(standardSync).withCatalog(expectedCatalog), actualStandardSync);
      // the notification function is being called with copy of the originalSync that does not contain the
      // updated catalog
      // This is ok as we only pass that object to get connectionId and connectionName
      verify(notificationHelper).notifySchemaPropagated(NOTIFICATION_SETTINGS, expectedDiff, WORKSPACE,
          apiPojoConverters.internalToConnectionRead(originalSync),
          source, EMAIL);
    }

    @Test
    void testAutoPropagateColumnsOnly()
        throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException,
        io.airbyte.config.persistence.ConfigNotFoundException {
      // See test above for why this part is necessary.
      final StandardSync originalSync = Jsons.clone(standardSync);
      final Field newField = Field.of(A_DIFFERENT_COLUMN, JsonSchemaType.STRING);
      final io.airbyte.api.model.generated.AirbyteCatalog catalogWithDiff = catalogConverter.toApi(
          io.airbyte.protocol.models.CatalogHelpers.createAirbyteCatalog(SHOES,
              Field.of(SKU, JsonSchemaType.STRING),
              newField),
          SOURCE_VERSION);

      final ConnectionAutoPropagateSchemaChange request = new ConnectionAutoPropagateSchemaChange()
          .connectionId(CONNECTION_ID)
          .workspaceId(WORKSPACE_ID)
          .catalogId(SOURCE_CATALOG_ID)
          .catalog(catalogWithDiff);

      final ConnectionAutoPropagateResult actualResult = connectionsHandler.applySchemaChange(request);

      final CatalogDiff expectedDiff =
          new CatalogDiff().addTransformsItem(new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().namespace(null).name(SHOES))
              .updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(new FieldTransform()
                  .addField(new FieldAdd().schema(Jsons.deserialize("{\"type\": \"string\"}")))
                  .fieldName(List.of(newField.getName()))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD))));
      assertEquals(expectedDiff, actualResult.getPropagatedDiff());
      verify(notificationHelper).notifySchemaPropagated(NOTIFICATION_SETTINGS, expectedDiff, WORKSPACE,
          apiPojoConverters.internalToConnectionRead(originalSync),
          source, EMAIL);
    }

    @Test
    void testSendingNotificationToManuallyApplySchemaChange()
        throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException,
        io.airbyte.config.persistence.ConfigNotFoundException {
      // Override the non-breaking changes preference to ignore so that the changes are not
      // auto-propagated, but needs to be manually applied.
      standardSync.setNonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE);
      when(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(standardSync);
      final StandardSync originalSync = Jsons.clone(standardSync);
      final Field newField = Field.of(A_DIFFERENT_COLUMN, JsonSchemaType.STRING);
      final io.airbyte.api.model.generated.AirbyteCatalog catalogWithDiff = catalogConverter.toApi(
          io.airbyte.protocol.models.CatalogHelpers.createAirbyteCatalog(SHOES,
              Field.of(SKU, JsonSchemaType.STRING),
              newField),
          SOURCE_VERSION);

      final ConnectionAutoPropagateSchemaChange request = new ConnectionAutoPropagateSchemaChange()
          .connectionId(CONNECTION_ID)
          .workspaceId(WORKSPACE_ID)
          .catalogId(SOURCE_CATALOG_ID)
          .catalog(catalogWithDiff);

      connectionsHandler.applySchemaChange(request);

      final CatalogDiff expectedDiff =
          new CatalogDiff().addTransformsItem(new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().namespace(null).name(SHOES))
              .updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(new FieldTransform()
                  .addField(new FieldAdd().schema(Jsons.deserialize("{\"type\": \"string\"}")))
                  .fieldName(List.of(newField.getName()))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD))));

      verify(notificationHelper).notifySchemaDiffToApply(NOTIFICATION_SETTINGS, expectedDiff, WORKSPACE,
          apiPojoConverters.internalToConnectionRead(originalSync),
          source, EMAIL);
    }

    @Test
    void diffCatalogGeneratesADiffAndUpdatesTheConnection()
        throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
      final Field newField = Field.of(A_DIFFERENT_COLUMN, JsonSchemaType.STRING);
      final var catalogWithDiff =
          io.airbyte.protocol.models.CatalogHelpers.createAirbyteCatalog(SHOES, Field.of(SKU, JsonSchemaType.STRING), newField);
      final ActorCatalog discoveredCatalog = new ActorCatalog()
          .withCatalog(Jsons.jsonNode(catalogWithDiff))
          .withCatalogHash("")
          .withId(UUID.randomUUID());
      when(catalogService.getActorCatalogById(DISCOVERED_CATALOG_ID)).thenReturn(discoveredCatalog);

      final CatalogDiff expectedDiff =
          new CatalogDiff().addTransformsItem(new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().namespace(null).name(SHOES))
              .updateStream(new StreamTransformUpdateStream().addFieldTransformsItem(new FieldTransform()
                  .addField(new FieldAdd().schema(Jsons.deserialize("{\"type\": \"string\"}")))
                  .fieldName(List.of(newField.getName()))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD))));

      final var result = connectionsHandler.diffCatalogAndConditionallyDisable(CONNECTION_ID, DISCOVERED_CATALOG_ID);

      assertEquals(expectedDiff, result.getCatalogDiff());
      assertEquals(false, result.getBreakingChange());

      final ArgumentCaptor<StandardSync> syncCaptor = ArgumentCaptor.forClass(StandardSync.class);
      verify(connectionService).writeStandardSync(syncCaptor.capture());
      final StandardSync savedSync = syncCaptor.getValue();
      assertNotEquals(Status.INACTIVE, savedSync.getStatus());
    }

    @Test
    @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
    void diffCatalogADisablesForBreakingChange()
        throws JsonValidationException, ConfigNotFoundException, IOException,
        io.airbyte.config.persistence.ConfigNotFoundException, NoSuchFieldException, IllegalAccessException {
      final ApplySchemaChangeHelper helper = mock(ApplySchemaChangeHelper.class);
      final java.lang.reflect.Field field = ConnectionsHandler.class.getDeclaredField("applySchemaChangeHelper");
      field.setAccessible(true);
      field.set(connectionsHandler, helper);

      when(helper.containsBreakingChange(any())).thenReturn(true);

      final var result = connectionsHandler.diffCatalogAndConditionallyDisable(CONNECTION_ID, SOURCE_CATALOG_ID);
      assertEquals(true, result.getBreakingChange());

      final ArgumentCaptor<StandardSync> syncCaptor = ArgumentCaptor.forClass(StandardSync.class);
      verify(connectionService).writeStandardSync(syncCaptor.capture());
      final StandardSync savedSync = syncCaptor.getValue();
      assertEquals(Status.INACTIVE, savedSync.getStatus());
    }

    @Test
    void diffCatalogDisablesForNonBreakingChangeIfConfiguredSo()
        throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
      // configure the sync to be disabled on non-breaking change
      standardSync = standardSync.withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.DISABLE);
      when(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(standardSync);

      final Field newField = Field.of(A_DIFFERENT_COLUMN, JsonSchemaType.STRING);
      final var catalogWithDiff =
          io.airbyte.protocol.models.CatalogHelpers.createAirbyteCatalog(SHOES, Field.of(SKU, JsonSchemaType.STRING), newField);
      final ActorCatalog discoveredCatalog = new ActorCatalog()
          .withCatalog(Jsons.jsonNode(catalogWithDiff))
          .withCatalogHash("")
          .withId(UUID.randomUUID());
      when(catalogService.getActorCatalogById(DISCOVERED_CATALOG_ID)).thenReturn(discoveredCatalog);

      final var result = connectionsHandler.diffCatalogAndConditionallyDisable(CONNECTION_ID, DISCOVERED_CATALOG_ID);

      assertEquals(false, result.getBreakingChange());

      final ArgumentCaptor<StandardSync> syncCaptor = ArgumentCaptor.forClass(StandardSync.class);
      verify(connectionService).writeStandardSync(syncCaptor.capture());
      final StandardSync savedSync = syncCaptor.getValue();
      assertEquals(Status.INACTIVE, savedSync.getStatus());
    }

    @Test
    void postprocessDiscoveredComposesDiffingAndSchemaPropagation()
        throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
      final var catalog = catalogConverter.toApi(Jsons.clone(airbyteCatalog), SOURCE_VERSION);
      final var diffResult = new SourceDiscoverSchemaRead().catalog(catalog);
      final var transform = new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(new StreamDescriptor().namespace(A_DIFFERENT_NAMESPACE).name(A_DIFFERENT_STREAM));
      final var propagatedDiff = new CatalogDiff().transforms(List.of(transform));
      final var autoPropResult = new ConnectionAutoPropagateResult().propagatedDiff(propagatedDiff);

      final var spiedConnectionsHandler = spy(connectionsHandler);
      doReturn(diffResult).when(spiedConnectionsHandler).diffCatalogAndConditionallyDisable(CONNECTION_ID, DISCOVERED_CATALOG_ID);
      doReturn(autoPropResult).when(spiedConnectionsHandler).applySchemaChange(CONNECTION_ID, WORKSPACE_ID, DISCOVERED_CATALOG_ID, catalog, true);

      final var result = spiedConnectionsHandler.postprocessDiscoveredCatalog(CONNECTION_ID, DISCOVERED_CATALOG_ID);

      assertEquals(propagatedDiff, result.getAppliedDiff());
    }

    @Test
    void postprocessDiscoveredComposesDiffingAndSchemaPropagationUsesMostRecentCatalog()
        throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
      final var catalog = catalogConverter.toApi(Jsons.clone(airbyteCatalog), SOURCE_VERSION);
      final var diffResult = new SourceDiscoverSchemaRead().catalog(catalog);
      final var transform = new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(new StreamDescriptor().namespace(A_DIFFERENT_NAMESPACE).name(A_DIFFERENT_STREAM));
      final var propagatedDiff = new CatalogDiff().transforms(List.of(transform));
      final var autoPropResult = new ConnectionAutoPropagateResult().propagatedDiff(propagatedDiff);

      final var mostRecentCatalogId = UUID.randomUUID();
      final var mostRecentCatalog = new ActorCatalogWithUpdatedAt().withId(mostRecentCatalogId);
      doReturn(Optional.of(mostRecentCatalog)).when(catalogService).getMostRecentSourceActorCatalog(SOURCE_ID);

      final var spiedConnectionsHandler = spy(connectionsHandler);
      doReturn(diffResult).when(spiedConnectionsHandler).diffCatalogAndConditionallyDisable(CONNECTION_ID, mostRecentCatalogId);
      doReturn(autoPropResult).when(spiedConnectionsHandler).applySchemaChange(CONNECTION_ID, WORKSPACE_ID, mostRecentCatalogId, catalog, true);

      final var result = spiedConnectionsHandler.postprocessDiscoveredCatalog(CONNECTION_ID, DISCOVERED_CATALOG_ID);

      assertEquals(propagatedDiff, result.getAppliedDiff());
    }

  }

  @Nested
  class ConnectionLastJobPerStream {

    @BeforeEach
    void setUp() {
      connectionsHandler = new ConnectionsHandler(
          streamRefreshesHandler,
          jobPersistence,
          catalogService,
          uuidGenerator,
          workspaceHelper,
          trackingClient,
          eventRunner,
          connectionHelper,
          featureFlagClient,
          actorDefinitionVersionHelper,
          connectorDefinitionSpecificationHandler,
          streamGenerationRepository,
          catalogGenerationSetter,
          catalogValidator,
          notificationHelper,
          streamStatusesService,
          connectionTimelineEventService,
          connectionTimelineEventHelper,
          statePersistence,
          sourceService,
          destinationService,
          connectionService,
          workspaceService,
          destinationCatalogGenerator,
          catalogConverter,
          applySchemaChangeHelper,
          apiPojoConverters,
          connectionSchedulerHelper,
          mapperSecretHelper);
    }

    @Test
    void testGetConnectionLastJobPerStream() throws IOException {
      final UUID connectionId = UUID.randomUUID();
      final Long jobId = 1L;
      final String stream1Name = "testStream1";
      final String stream1Namespace = "testNamespace1";
      final String stream2Name = "testStream2";
      final io.airbyte.config.StreamDescriptor stream1Descriptor = new io.airbyte.config.StreamDescriptor()
          .withName(stream1Name)
          .withNamespace(stream1Namespace);
      final io.airbyte.config.StreamDescriptor stream2Descriptor = new io.airbyte.config.StreamDescriptor()
          .withName(stream2Name)
          .withNamespace(null);
      final Instant createdAt = Instant.now();
      final Instant startedAt = createdAt.plusSeconds(10);
      final Instant updatedAt = startedAt.plusSeconds(10);
      final long bytesCommitted1 = 12345L;
      final long bytesEmitted1 = 23456L;
      final long recordsCommitted1 = 100L;
      final long recordsEmitted1 = 200L;
      final long bytesCommitted2 = 912345L;
      final long bytesEmitted2 = 923456L;
      final long recordsCommitted2 = 9100L;
      final long recordsEmitted2 = 9200L;
      final ConfigType configType = ConfigType.SYNC;
      final JobConfigType jobConfigType = JobConfigType.SYNC;
      final JobStatus jobStatus = JobStatus.SUCCEEDED;
      final io.airbyte.api.model.generated.JobStatus apiJobStatus = io.airbyte.api.model.generated.JobStatus.SUCCEEDED;
      final JobAggregatedStats jobAggregatedStats = new JobAggregatedStats()
          .bytesCommitted(bytesCommitted1 + bytesCommitted2)
          .bytesEmitted(bytesEmitted1 + bytesEmitted2)
          .recordsCommitted(recordsCommitted1 + recordsCommitted2)
          .recordsEmitted(recordsEmitted1 + recordsEmitted2);
      final Job job = new Job(
          jobId,
          configType,
          connectionId.toString(),
          null,
          null,
          jobStatus,
          startedAt.toEpochMilli(),
          createdAt.toEpochMilli(),
          updatedAt.toEpochMilli());
      final List<Job> jobList = List.of(job);
      final JobRead jobRead = new JobRead()
          .id(job.getId())
          .configType(jobConfigType)
          .status(apiJobStatus)
          .aggregatedStats(jobAggregatedStats)
          .createdAt(job.getCreatedAtInSecond())
          .updatedAt(job.getUpdatedAtInSecond())
          .startedAt(job.getStartedAtInSecond().orElseThrow())
          .streamAggregatedStats(List.of(
              new StreamStats()
                  .streamName(stream1Name)
                  .streamNamespace(stream1Namespace)
                  .bytesCommitted(bytesCommitted1)
                  .recordsCommitted(recordsCommitted1),
              new StreamStats()
                  .streamName(stream2Name)
                  .streamNamespace(null)
                  .bytesCommitted(bytesCommitted2)
                  .recordsCommitted(recordsCommitted2)));
      final JobWithAttemptsRead jobWithAttemptsRead = new JobWithAttemptsRead()
          .job(jobRead);

      final ConnectionLastJobPerStreamRequestBody apiReq = new ConnectionLastJobPerStreamRequestBody()
          .connectionId(connectionId);

      final ConnectionLastJobPerStreamReadItem stream1ReadItem = new ConnectionLastJobPerStreamReadItem()
          .streamName(stream1Name)
          .streamNamespace(stream1Namespace)
          .jobId(jobId)
          .configType(jobConfigType)
          .jobStatus(apiJobStatus)
          .bytesCommitted(bytesCommitted1)
          .recordsCommitted(recordsCommitted1)
          .startedAt(startedAt.toEpochMilli())
          .endedAt(updatedAt.toEpochMilli());

      final ConnectionLastJobPerStreamReadItem stream2ReadItem = new ConnectionLastJobPerStreamReadItem()
          .streamName(stream2Name)
          .streamNamespace(null)
          .jobId(jobId)
          .configType(jobConfigType)
          .jobStatus(apiJobStatus)
          .bytesCommitted(bytesCommitted2)
          .recordsCommitted(recordsCommitted2)
          .startedAt(startedAt.toEpochMilli())
          .endedAt(updatedAt.toEpochMilli());

      when(streamStatusesService.getLastJobIdWithStatsByStream(connectionId))
          .thenReturn(Map.of(
              stream1Descriptor, jobId,
              stream2Descriptor, jobId));

      when(jobPersistence.listJobsLight(Set.of(jobId))).thenReturn(jobList);

      try (final MockedStatic<StatsAggregationHelper> mockStatsAggregationHelper = Mockito.mockStatic(StatsAggregationHelper.class)) {
        mockStatsAggregationHelper.when(() -> StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(eq(jobList), eq(jobPersistence)))
            .thenReturn(Map.of(jobId, jobWithAttemptsRead));

        final List<ConnectionLastJobPerStreamReadItem> expectedStream1And2 = List.of(stream1ReadItem, stream2ReadItem);

        assertEquals(expectedStream1And2, connectionsHandler.getConnectionLastJobPerStream(apiReq));
      }
    }

  }

}
