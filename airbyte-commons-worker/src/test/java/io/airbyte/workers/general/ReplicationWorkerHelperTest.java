/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteAnalyticsTraceMessage;
import io.airbyte.protocol.models.AirbyteLogMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReplicationWorkerHelperTest {

  private ReplicationWorkerHelper replicationWorkerHelper;
  private AirbyteMapper mapper;
  private SyncStatsTracker syncStatsTracker;
  private AirbyteMessageTracker messageTracker;

  private AnalyticsMessageTracker analyticsMessageTracker;

  @BeforeEach
  void setUp() {
    mapper = mock(AirbyteMapper.class);
    syncStatsTracker = mock(SyncStatsTracker.class);
    messageTracker = mock(AirbyteMessageTracker.class);
    analyticsMessageTracker = mock(AnalyticsMessageTracker.class);
    when(messageTracker.getSyncStatsTracker()).thenReturn(syncStatsTracker);
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        mock(AirbyteMessageDataExtractor.class),
        mock(FieldSelector.class),
        mapper,
        messageTracker,
        mock(SyncPersistence.class),
        mock(ReplicationAirbyteMessageEventPublishingHelper.class),
        mock(ThreadedTimeTracker.class),
        mock(VoidCallable.class),
        mock(WorkloadApi.class),
        new WorkloadIdGenerator(),
        false, analyticsMessageTracker));
  }

  @Test
  void testGetReplicationOutput() throws JsonProcessingException {
    // Need to pass in a replication context
    replicationWorkerHelper.initialize(
        new ReplicationContext(true, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 0L, 1, UUID.randomUUID()),
        mock(ReplicationFeatureFlags.class),
        mock(Path.class));
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
  void testAnalyticsMessageHandling() {
    final ReplicationContext context =
        new ReplicationContext(true, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 0L, 1, UUID.randomUUID());
    // Need to pass in a replication context
    replicationWorkerHelper.initialize(
        context,
        mock(ReplicationFeatureFlags.class),
        mock(Path.class));
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

}
