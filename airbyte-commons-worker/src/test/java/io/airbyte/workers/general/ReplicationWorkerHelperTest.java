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

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.internal.AirbyteMapper;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReplicationWorkerHelperTest {

  private ReplicationWorkerHelper replicationWorkerHelper;
  private final AirbyteMapper mapper = mock(AirbyteMapper.class);

  @BeforeEach
  void setUp() {
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(
        null,
        null,
        mapper,
        null,
        null,
        null,
        null,
        null));
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
