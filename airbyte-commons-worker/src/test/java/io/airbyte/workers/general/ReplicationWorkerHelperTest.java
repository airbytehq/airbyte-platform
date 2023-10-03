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

import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReplicationWorkerHelperTest {

  private ReplicationWorkerHelper replicationWorkerHelper;
  private final AirbyteMapper mapper = mock(AirbyteMapper.class);
  private final AirbyteMessageDataExtractor extractor = mock(AirbyteMessageDataExtractor.class);
  private final FieldSelector fieldSelector = mock(FieldSelector.class);
  private final AirbyteMessageTracker messageTracker = mock(AirbyteMessageTracker.class);
  private final SyncPersistence syncPersistence = mock(SyncPersistence.class);
  private final ReplicationAirbyteMessageEventPublishingHelper eventPublishingHelper = mock(ReplicationAirbyteMessageEventPublishingHelper.class);
  private final ThreadedTimeTracker timeTracker = mock(ThreadedTimeTracker.class);
  private final VoidCallable running = mock(VoidCallable.class);

  @BeforeEach
  void setUp() {
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        extractor,
        fieldSelector,
        mapper,
        messageTracker,
        syncPersistence,
        eventPublishingHelper,
        timeTracker,
        running));
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
