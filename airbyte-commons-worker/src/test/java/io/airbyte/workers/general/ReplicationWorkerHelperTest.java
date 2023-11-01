/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
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

  @BeforeEach
  void setUp() {
    mapper = mock(AirbyteMapper.class);
    syncStatsTracker = mock(SyncStatsTracker.class);
    messageTracker = mock(AirbyteMessageTracker.class);
    when(messageTracker.getSyncStatsTracker()).thenReturn(syncStatsTracker);
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        mock(AirbyteMessageDataExtractor.class),
        mock(FieldSelector.class),
        mapper,
        messageTracker,
        mock(SyncPersistence.class),
        mock(ReplicationAirbyteMessageEventPublishingHelper.class),
        mock(ThreadedTimeTracker.class),
        mock(VoidCallable.class)));
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

    var summary = replicationWorkerHelper.getReplicationOutput();
    assertEquals(50L, summary.getReplicationAttemptSummary().getBytesSynced());
    assertEquals(5L, summary.getReplicationAttemptSummary().getRecordsSynced());
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
