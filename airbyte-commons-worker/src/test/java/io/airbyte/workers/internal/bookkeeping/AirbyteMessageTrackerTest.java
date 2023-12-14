/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.config.FailureReason;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage.Type;
import io.airbyte.protocol.models.Config;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AirbyteMessageTrackerTest {

  private AirbyteMessageTracker messageTracker;
  @Mock
  private SyncStatsTracker syncStatsTracker;

  @BeforeEach
  void setup() {
    this.messageTracker =
        new AirbyteMessageTracker(syncStatsTracker, new EnvVariableFeatureFlags(), "airbyte/source-image", "airbyte/destination-image", false);
  }

  @Test
  void testAcceptFromSourceTraceEstimate() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.ESTIMATE);

    messageTracker.acceptFromSource(trace);

    verify(syncStatsTracker).updateEstimates(trace.getTrace().getEstimate());
  }

  @Test
  void testAcceptFromSourceTraceError() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.ERROR);

    messageTracker.acceptFromSource(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromSourceTraceStreamStatus() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.STREAM_STATUS);

    messageTracker.acceptFromSource(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromSourceTraceEstimateOther() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), (Type) null);

    messageTracker.acceptFromSource(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromSourceRecord() {
    final AirbyteMessage record = AirbyteMessageUtils.createRecordMessage("stream 1", 123);

    messageTracker.acceptFromSource(record);

    verify(syncStatsTracker).updateStats(record.getRecord());
  }

  @Test
  void testAcceptFromSourceState() {
    final AirbyteMessage state = AirbyteMessageUtils.createStateMessage(2);

    messageTracker.acceptFromSource(state);

    verify(syncStatsTracker).updateSourceStatesStats(state.getState(), false);
  }

  @Test
  void testAcceptFromSourceControl() {
    final AirbyteMessage control = AirbyteMessageUtils.createConfigControlMessage(mock(Config.class), 0.0);

    messageTracker.acceptFromSource(control);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationTraceEstimate() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.ESTIMATE);

    messageTracker.acceptFromDestination(trace);

    verify(syncStatsTracker).updateEstimates(trace.getTrace().getEstimate());
  }

  @Test
  void testAcceptFromDestinationTraceAnalytics() {
    final AirbyteMessage trace = AirbyteMessageUtils.createAnalyticsTraceMessage("abc", "def");

    messageTracker.acceptFromDestination(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromSourceTraceAnalytics() {
    final AirbyteMessage trace = AirbyteMessageUtils.createAnalyticsTraceMessage("abc", "def");

    messageTracker.acceptFromSource(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationTraceError() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.ERROR);

    messageTracker.acceptFromDestination(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationTraceStreamStatus() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), Type.STREAM_STATUS);

    messageTracker.acceptFromDestination(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationTraceEstimateOther() {
    final AirbyteMessage trace = AirbyteMessageUtils.createStatusTraceMessage(mock(StreamDescriptor.class), (Type) null);

    messageTracker.acceptFromDestination(trace);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationRecord() {
    final AirbyteMessage record = AirbyteMessageUtils.createRecordMessage("record 1", 123);

    messageTracker.acceptFromDestination(record);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testAcceptFromDestinationState() {
    final AirbyteMessage state = AirbyteMessageUtils.createStateMessage(2);

    messageTracker.acceptFromDestination(state);

    verify(syncStatsTracker).updateDestinationStateStats(state.getState(), false);
  }

  @Test
  void testAcceptFromDestinationControl() {
    final AirbyteMessage control = AirbyteMessageUtils.createConfigControlMessage(mock(Config.class), 0.0);

    messageTracker.acceptFromDestination(control);

    verifyNoInteractions(syncStatsTracker);
  }

  @Test
  void testErrorTraceMessageFailureWithMultipleTraceErrors() {
    final AirbyteMessage srcMsg1 = AirbyteMessageUtils.createErrorMessage("source trace 1", 123.0);
    final AirbyteMessage srcMsg2 = AirbyteMessageUtils.createErrorMessage("source trace 2", 124.0);
    final AirbyteMessage dstMsg1 = AirbyteMessageUtils.createErrorMessage("dest trace 1", 125.0);
    final AirbyteMessage dstMsg2 = AirbyteMessageUtils.createErrorMessage("dest trace 2", 126.0);

    messageTracker.acceptFromSource(srcMsg1);
    messageTracker.acceptFromSource(srcMsg2);
    messageTracker.acceptFromDestination(dstMsg1);
    messageTracker.acceptFromDestination(dstMsg2);

    final FailureReason failureReason = FailureHelper.sourceFailure(srcMsg1.getTrace(), Long.valueOf(123), 1);
    assertEquals(messageTracker.errorTraceMessageFailure(123L, 1), failureReason);
  }

  @Test
  void testErrorTraceMessageFailureWithOneTraceError() {
    final AirbyteMessage destMessage = AirbyteMessageUtils.createErrorMessage("dest trace 1", 125.0);
    messageTracker.acceptFromDestination(destMessage);

    final FailureReason failureReason = FailureHelper.destinationFailure(destMessage.getTrace(), Long.valueOf(123), 1);
    assertEquals(messageTracker.errorTraceMessageFailure(123L, 1), failureReason);
  }

  @Test
  void testErrorTraceMessageFailureWithNoTraceErrors() {
    assertNull(messageTracker.errorTraceMessageFailure(123L, 1));
  }

}
