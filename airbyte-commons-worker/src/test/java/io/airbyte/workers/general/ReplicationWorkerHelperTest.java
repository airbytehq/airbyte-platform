/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.WorkloadApiClient;
import io.airbyte.api.client.generated.ActorDefinitionVersionApi;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.config.State;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteAnalyticsTraceMessage;
import io.airbyte.protocol.models.AirbyteLogMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
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
import io.airbyte.workload.api.client.generated.WorkloadApi;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        mock(FieldSelector.class),
        mapper,
        messageTracker,
        syncPersistence,
        replicationAirbyteMessageEventPublishingHelper,
        mock(ThreadedTimeTracker.class),
        mock(VoidCallable.class),
        workloadApiClient,
        false,
        analyticsMessageTracker,
        Optional.empty(),
        airbyteApiClient,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory));
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
    final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog().withAdditionalProperty("test", "test");
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
    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        mock(ConfiguredAirbyteCatalog.class),
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

    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        mock(ConfiguredAirbyteCatalog.class),
        mock(State.class));

    final AirbyteMessage message = mock(AirbyteMessage.class);

    replicationWorkerHelper.processMessageFromSource(message);

    verify(streamStatusTracker, times(1)).track(message);
  }

  @Test
  void callsStreamStatusTrackerOnDestinationMessage() throws IOException {
    mockSupportRefreshes(true);

    replicationWorkerHelper.initialize(
        replicationContext,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class),
        mock(ConfiguredAirbyteCatalog.class),
        mock(State.class));

    final AirbyteMessage message = mock(AirbyteMessage.class);
    when(mapper.revertMap(message)).thenReturn(message);

    replicationWorkerHelper.processMessageFromDestination(message);

    verify(streamStatusTracker, times(1)).track(message);
  }

  private void mockSupportRefreshes(final boolean supportsRefreshes) throws IOException {
    when(actorDefinitionVersionApi.resolveActorDefinitionVersionByTag(any())).thenReturn(
        new ResolveActorDefinitionVersionResponse(
            UUID.randomUUID(),
            "dockerRepository",
            "dockerImageTag",
            supportsRefreshes));
  }

}
