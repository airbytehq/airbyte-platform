/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.workers.test_utils.TestConfigHelpers.DESTINATION_IMAGE;
import static io.airbyte.workers.test_utils.TestConfigHelpers.SOURCE_IMAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ActorDefinitionVersionApi;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.MapperConfig;
import io.airbyte.config.State;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.mappers.application.RecordMapper;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteAnalyticsTraceMessage;
import io.airbyte.protocol.models.AirbyteLogMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ReplicationWorkerHelperTest {

  private ReplicationWorkerHelper replicationWorkerHelper;
  private AirbyteMapper mapper;
  private SyncStatsTracker syncStatsTracker;
  private AirbyteMessageTracker messageTracker;
  private SyncPersistence syncPersistence;
  private AnalyticsMessageTracker analyticsMessageTracker;
  private StreamStatusCompletionTracker streamStatusCompletionTracker;
  private StreamStatusTracker streamStatusTracker;
  private StreamStatusTrackerFactory streamStatusTrackerFactory;
  private WorkloadApiClient workloadApiClient;
  private AirbyteApiClient airbyteApiClient;
  private ActorDefinitionVersionApi actorDefinitionVersionApi;
  private ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  private RecordMapper recordMapper;
  private FeatureFlagClient featureFlagClient;
  private DestinationCatalogGenerator destinationCatalogGenerator;

  private final ReplicationContext replicationContext = new ReplicationContext(true, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 0L,
      1, UUID.randomUUID(), SOURCE_IMAGE, DESTINATION_IMAGE, UUID.randomUUID(), UUID.randomUUID());

  @BeforeEach
  void setUp() {
    mapper = mock(AirbyteMapper.class);
    syncStatsTracker = mock(SyncStatsTracker.class);
    syncPersistence = mock(SyncPersistence.class);
    messageTracker = mock(AirbyteMessageTracker.class);
    analyticsMessageTracker = mock(AnalyticsMessageTracker.class);
    streamStatusCompletionTracker = mock(StreamStatusCompletionTracker.class);
    replicationAirbyteMessageEventPublishingHelper = mock(ReplicationAirbyteMessageEventPublishingHelper.class);
    streamStatusTracker = mock(StreamStatusTracker.class);
    streamStatusTrackerFactory = mock(StreamStatusTrackerFactory.class);
    workloadApiClient = mock(WorkloadApiClient.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    when(messageTracker.getSyncStatsTracker()).thenReturn(syncStatsTracker);
    when(workloadApiClient.getWorkloadApi()).thenReturn(mock(WorkloadApi.class));
    when(airbyteApiClient.getDestinationApi()).thenReturn(mock(DestinationApi.class));
    when(airbyteApiClient.getSourceApi()).thenReturn(mock(SourceApi.class));
    actorDefinitionVersionApi = mock(ActorDefinitionVersionApi.class);
    when(airbyteApiClient.getActorDefinitionVersionApi()).thenReturn(actorDefinitionVersionApi);
    when(streamStatusTrackerFactory.create(any())).thenReturn(streamStatusTracker);
    recordMapper = mock(RecordMapper.class);
    featureFlagClient = mock(TestClient.class);
    destinationCatalogGenerator = mock(DestinationCatalogGenerator.class);
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        mock(FieldSelector.class),
        mapper,
        messageTracker,
        syncPersistence,
        replicationAirbyteMessageEventPublishingHelper,
        mock(ThreadedTimeTracker.class),
        mock(VoidCallable.class),
        workloadApiClient,
        analyticsMessageTracker,
        "workload-id",
        airbyteApiClient,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory,
        recordMapper,
        featureFlagClient,
        destinationCatalogGenerator));
  }

  @AfterEach
  void tearDown() {
    Mockito.framework().clearInlineMocks();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetReplicationOutput(final boolean supportRefreshes) throws IOException {
    mockSupportRefreshes(supportRefreshes);
    // Need to pass in a replication context
    final ConfiguredAirbyteCatalog catalog = buildConfiguredAirbyteCatalog();
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));
    verify(streamStatusCompletionTracker).startTracking(catalog, supportRefreshes);
    // Need to have a configured catalog for getReplicationOutput
    replicationWorkerHelper.startDestination(
        mock(AirbyteDestination.class),
        new ReplicationInput().withCatalog(new ConfiguredAirbyteCatalog()),
        mock(Path.class));

    when(syncStatsTracker.getTotalBytesEmitted()).thenReturn(100L);
    when(syncStatsTracker.getTotalRecordsEmitted()).thenReturn(10L);
    when(syncStatsTracker.getTotalBytesCommitted()).thenReturn(50L);
    when(syncStatsTracker.getTotalRecordsCommitted()).thenReturn(5L);

    final var summary = replicationWorkerHelper.getReplicationOutput();
    assertEquals(50L, summary.getReplicationAttemptSummary().getBytesSynced());
    assertEquals(5L, summary.getReplicationAttemptSummary().getRecordsSynced());
  }

  @Test
  void testAnalyticsMessageHandling() throws IOException {
    mockSupportRefreshes(false);
    // Need to pass in a replication context
    final ConfiguredAirbyteCatalog catalog = mock(ConfiguredAirbyteCatalog.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));
    // Need to have a configured catalog for getReplicationOutput
    replicationWorkerHelper.startDestination(
        mock(AirbyteDestination.class),
        new ReplicationInput().withCatalog(new ConfiguredAirbyteCatalog()),
        mock(Path.class));

    replicationWorkerHelper.startSource(
        mock(AirbyteSource.class),
        new ReplicationInput().withCatalog(new ConfiguredAirbyteCatalog()),
        mock(Path.class));

    final AirbyteMessage sourceMessage = new AirbyteMessage().withType(Type.TRACE).withTrace(new AirbyteTraceMessage()
        .withType(AirbyteTraceMessage.Type.ANALYTICS).withAnalytics(new AirbyteAnalyticsTraceMessage().withType("from").withValue("source")));
    final AirbyteMessage logMessage = new AirbyteMessage().withType(Type.LOG).withLog(new AirbyteLogMessage().withMessage("test"));
    final AirbyteMessage destinationMessage = new AirbyteMessage().withType(Type.TRACE).withTrace(new AirbyteTraceMessage()
        .withType(AirbyteTraceMessage.Type.ANALYTICS).withAnalytics(new AirbyteAnalyticsTraceMessage().withType("from").withValue("destination")));
    when(mapper.mapMessage(any())).thenAnswer(i -> i.getArgument(0));
    when(mapper.revertMap(any())).thenAnswer(i -> i.getArgument(0));

    replicationWorkerHelper.processMessageFromSource(sourceMessage);
    // this shouldn't be passed to the message tracker
    replicationWorkerHelper.processMessageFromSource(logMessage);
    replicationWorkerHelper.processMessageFromDestination(destinationMessage);

    replicationWorkerHelper.endOfReplication();
    verify(analyticsMessageTracker, times(1)).addMessage(sourceMessage, AirbyteMessageOrigin.SOURCE);
    verify(analyticsMessageTracker, times(1)).addMessage(destinationMessage, AirbyteMessageOrigin.DESTINATION);
    verify(analyticsMessageTracker, times(1)).flush();
  }

  @Test
  void testMessageIsMappedAfterProcessing() {
    final AirbyteMessage sourceRawMessage = mock(AirbyteMessage.class);
    final AirbyteMessage mappedSourceMessage = mock(AirbyteMessage.class);

    doReturn(sourceRawMessage).when(replicationWorkerHelper).internalProcessMessageFromSource(sourceRawMessage);
    when(mapper.mapMessage(sourceRawMessage)).thenReturn(mappedSourceMessage);

    final Optional<AirbyteMessage> processedMessageFromSource = replicationWorkerHelper.processMessageFromSource(sourceRawMessage);

    assertEquals(Optional.of(mappedSourceMessage), processedMessageFromSource);
  }

  @Test
  void testMessageMapIsRevertedBeforeProcessing() {
    final AirbyteMessage destinationRawMessage = mock(AirbyteMessage.class);
    final AirbyteMessage mapRevertedDestinationMessage = mock(AirbyteMessage.class);

    when(mapper.revertMap(destinationRawMessage)).thenReturn(mapRevertedDestinationMessage);
    doNothing().when(replicationWorkerHelper).internalProcessMessageFromDestination(mapRevertedDestinationMessage);

    replicationWorkerHelper.processMessageFromDestination(destinationRawMessage);

    verify(replicationWorkerHelper, times(1)).internalProcessMessageFromDestination(mapRevertedDestinationMessage);
  }

  @Test
  void callsStreamStatusTrackerOnSourceMessage() throws IOException {
    mockSupportRefreshes(true);
    final ConfiguredAirbyteCatalog catalog = mock(ConfiguredAirbyteCatalog.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));

    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));

    final AirbyteMessage message = mock(AirbyteMessage.class);

    replicationWorkerHelper.processMessageFromSource(message);

    verify(streamStatusTracker, times(1)).track(message);
  }

  @Test
  void callsStreamStatusTrackerOnDestinationMessage() throws IOException {
    mockSupportRefreshes(true);
    final ConfiguredAirbyteCatalog catalog = mock(ConfiguredAirbyteCatalog.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));

    final AirbyteMessage message = mock(AirbyteMessage.class);
    when(mapper.revertMap(message)).thenReturn(message);

    replicationWorkerHelper.processMessageFromDestination(message);

    verify(streamStatusTracker, times(1)).track(message);
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest
  void testSupportRefreshesIsPassed(final boolean supportRefreshes) throws Exception {
    mockSupportRefreshes(supportRefreshes);
    // Need to pass in a replication context
    final ConfiguredAirbyteCatalog catalog = buildConfiguredAirbyteCatalog();
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));

    final ArgumentCaptor<WorkerDestinationConfig> configCaptor = ArgumentCaptor.forClass(WorkerDestinationConfig.class);
    final AirbyteDestination destination = mock(AirbyteDestination.class);

    final ReplicationInput input = new ReplicationInput().withCatalog(new ConfiguredAirbyteCatalog());
    replicationWorkerHelper.startDestination(destination, input, mock(Path.class));

    verify(destination).start(configCaptor.capture(), any());
    assertEquals(supportRefreshes, configCaptor.getValue().getSupportRefreshes());
  }

  @Test
  void testApplyTransformationNoMapper() throws IOException {
    mockSupportRefreshes(false);
    final ConfiguredAirbyteCatalog catalog = mock(ConfiguredAirbyteCatalog.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    // Need to pass in a replication context
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));

    final AirbyteMessage recordMessage = new AirbyteMessage().withType(Type.RECORD)
        .withRecord(new AirbyteRecordMessage().withStream("stream").withData(Jsons.jsonNode(Map.of("column", "value"))));
    final AirbyteMessage copiedRecordMessage = Jsons.clone(recordMessage);

    replicationWorkerHelper.applyTransformationMappers(new AirbyteJsonRecordAdapter(recordMessage));

    assertEquals(copiedRecordMessage, recordMessage);
    verifyNoInteractions(recordMapper);
  }

  @Test
  void testApplyTransformationMapper() throws IOException {
    mockSupportRefreshes(false);

    final ConfiguredAirbyteCatalog catalog = mock(ConfiguredAirbyteCatalog.class);
    final ConfiguredAirbyteStream stream = mock(ConfiguredAirbyteStream.class);
    final List<MapperConfig> mappers = List.of(new MapperConfig() {

      @NotNull
      @Override
      public String name() {
        return "test";
      }

      @Nullable
      @Override
      public String documentationUrl() {
        return null;
      }

      @Nullable
      @Override
      public UUID id() {
        return null;
      }

      @NotNull
      @Override
      public Object config() {
        return Map.of();
      }

    });

    when(stream.getStreamDescriptor()).thenReturn(new StreamDescriptor().withName("stream"));
    when(stream.getMappers()).thenReturn(mappers);
    when(catalog.getStreams()).thenReturn(List.of(stream));
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(catalog, Map.of()));
    // Need to pass in a replication context
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        catalog,
        mock(State.class));

    final AirbyteMessage recordMessage =
        new AirbyteMessage().withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream("stream").withData(Jsons.jsonNode(Map.of("column", "value"))));
    final AirbyteJsonRecordAdapter recordAdapter = new AirbyteJsonRecordAdapter(recordMessage);

    replicationWorkerHelper.applyTransformationMappers(recordAdapter);

    verify(recordMapper).applyMappers(recordAdapter, mappers);
  }

  private void mockSupportRefreshes(final boolean supportsRefreshes) throws IOException {
    when(actorDefinitionVersionApi.resolveActorDefinitionVersionByTag(any())).thenReturn(
        new ResolveActorDefinitionVersionResponse(
            UUID.randomUUID(),
            "dockerRepository",
            "dockerImageTag",
            supportsRefreshes,
            false));
  }

  private ConfiguredAirbyteCatalog buildConfiguredAirbyteCatalog() {
    return new ConfiguredAirbyteCatalog();
  }

}
