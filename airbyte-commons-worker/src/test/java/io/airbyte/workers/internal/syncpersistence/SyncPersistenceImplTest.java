/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.syncpersistence;

import static io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType.LEGACY;
import static io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType.STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.StreamState;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.internal.bookkeeping.ParallelStreamStatsTracker;
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker;
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.CollectionAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SyncPersistenceImplTest {

  private final long flushPeriod = 10;

  private SyncPersistenceImpl syncPersistence;
  private SyncStatsTracker syncStatsTracker;
  private StateApi stateApi;
  private AttemptApi attemptApi;
  private ScheduledExecutorService executorService;
  private ArgumentCaptor<Runnable> actualFlushMethod;

  private UUID connectionId;
  private Long jobId;
  private Integer attemptNumber;
  private AirbyteApiClient airbyteApiClient;

  @BeforeEach
  void beforeEach() {
    connectionId = UUID.randomUUID();
    jobId = (long) (Math.random() * Long.MAX_VALUE);
    attemptNumber = (int) (Math.random() * Integer.MAX_VALUE);
    airbyteApiClient = mock(AirbyteApiClient.class);

    // Setting up an ArgumentCaptor to be able to manually trigger the actual flush method rather than
    // relying on the ScheduledExecutorService and having to deal with Thread.sleep in the tests.
    actualFlushMethod = ArgumentCaptor.forClass(Runnable.class);

    // Wire the executor service with arg captures
    executorService = mock(ScheduledExecutorService.class);
    when(executorService.scheduleAtFixedRate(actualFlushMethod.capture(), eq(0L), eq(flushPeriod), eq(TimeUnit.SECONDS)))
        .thenReturn(mock(ScheduledFuture.class));

    syncStatsTracker = new ParallelStreamStatsTracker(mock(), mock(), false);

    // Setting syncPersistence
    stateApi = mock(StateApi.class);
    attemptApi = mock(AttemptApi.class);
    when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
    when(airbyteApiClient.getStateApi()).thenReturn(stateApi);

    syncPersistence = new SyncPersistenceImpl(airbyteApiClient, new StateAggregatorFactory(), syncStatsTracker, executorService,
        flushPeriod, new RetryWithJitterConfig(1, 1, 4),
        connectionId, jobId, attemptNumber);
  }

  @AfterEach
  void afterEach() {
    reset(stateApi, attemptApi);
    syncPersistence.close();
  }

  @Test
  void testHappyPath() throws IOException {
    final AirbyteStateMessage stateA1 = getStreamState("A", 1);
    syncPersistence.accept(connectionId, stateA1);
    clearInvocations(executorService, stateApi);

    // Simulating the expected flush execution
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateA1));
    clearInvocations(stateApi);

    final AirbyteStateMessage stateB1 = getStreamState("B", 1);
    final AirbyteStateMessage stateC2 = getStreamState("C", 2);
    syncPersistence.accept(connectionId, stateB1);
    syncPersistence.accept(connectionId, stateC2);

    // This should only happen the first time before we schedule the task
    verify(stateApi, never()).getState(any());

    // Forcing a second flush
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateB1, stateC2));
    clearInvocations(stateApi);

    // Forcing another flush without data to flush
    actualFlushMethod.getValue().run();
    verify(stateApi, never()).createOrUpdateState(any());
    clearInvocations(stateApi);

    // scheduleAtFixedRate should not have received any other calls
    verify(executorService, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
  }

  @Test
  void testFlushWithApiFailures() throws IOException {
    final AirbyteStateMessage stateF1 = getStreamState("F", 1);
    syncPersistence.accept(connectionId, stateF1);

    // Set API call to fail
    when(stateApi.createOrUpdateState(any())).thenThrow(new IOException());

    // Flushing
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateF1));
    clearInvocations(stateApi);

    // Adding more states
    final AirbyteStateMessage stateG1 = getStreamState("G", 1);
    syncPersistence.accept(connectionId, stateG1);

    // Flushing again
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateF1, stateG1));
    clearInvocations(stateApi);

    // Adding more states
    final AirbyteStateMessage stateF2 = getStreamState("F", 2);
    syncPersistence.accept(connectionId, stateF2);

    // Flushing again
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateF2, stateG1));
    clearInvocations(stateApi);

    // Clear the error state from the API
    reset(stateApi);

    // Flushing again
    actualFlushMethod.getValue().run();
    verifyStateUpdateApiCall(List.of(stateF2, stateG1));
    clearInvocations(stateApi);

    // Sanity check Flushing again should not trigger an API call since all the data has been
    // successfully flushed
    actualFlushMethod.getValue().run();
    verify(stateApi, never()).createOrUpdateState(any());
  }

  @Test
  void testStatsFlushBasicEmissions() throws IOException {
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, getStreamState("a", 1));

    actualFlushMethod.getValue().run();
    verify(stateApi).createOrUpdateState(any());
    verify(attemptApi).saveStats(any());
    clearInvocations(stateApi, attemptApi);

    // We emit stats even if there is no state to persist
    syncPersistence.updateStats(new AirbyteRecordMessage().withStream("stream1"));
    actualFlushMethod.getValue().run();
    verify(stateApi, never()).createOrUpdateState(any());
    verify(attemptApi).saveStats(any());
  }

  @Test
  void testStatsAreNotPersistedWhenStateFails() throws IOException {
    // We should not save stats if persist state failed
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, getStreamState("b", 2));
    when(stateApi.createOrUpdateState(any())).thenThrow(new IOException());
    actualFlushMethod.getValue().run();
    verify(stateApi).createOrUpdateState(any());
    verify(attemptApi, never()).saveStats(any());
    clearInvocations(stateApi, attemptApi);
    reset(stateApi);

    // Next sync should attempt to flush everything
    actualFlushMethod.getValue().run();
    verify(stateApi).createOrUpdateState(any());
    verify(attemptApi).saveStats(any());
  }

  @Test
  void testStatsFailuresAreRetriedOnFollowingRunsEvenWithoutNewStates() throws IOException {
    // If we failed to save stats, we should retry on the next schedule even if there were no new states
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, getStreamState("a", 3));
    when(attemptApi.saveStats(any())).thenThrow(new IOException());
    actualFlushMethod.getValue().run();
    verify(stateApi).createOrUpdateState(any());
    verify(attemptApi).saveStats(any());

    clearInvocations(stateApi, attemptApi);
    reset(attemptApi);

    actualFlushMethod.getValue().run();
    verify(stateApi, never()).createOrUpdateState(any());
    verify(attemptApi).saveStats(any());
  }

  @Test
  void startsFlushThreadOnInit() {
    // syncPersistence is created and init in the @BeforeEach block
    verify(executorService).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(flushPeriod), eq(TimeUnit.SECONDS));
  }

  @Test
  void statsDontPersistIfTheresBeenNoChanges() throws IOException {
    // update stats
    syncPersistence.updateStats(new AirbyteRecordMessage().withStream("stream1"));

    // stats have updated so we should save
    actualFlushMethod.getValue().run();
    verify(attemptApi).saveStats(any());
    clearInvocations(stateApi, attemptApi);

    // stats have NOT updated so we should not save
    actualFlushMethod.getValue().run();
    verify(attemptApi, never()).saveStats(any());
    clearInvocations(stateApi, attemptApi);

    // update stats for different stream
    syncPersistence.updateStats(new AirbyteRecordMessage().withStream("stream2").withNamespace("other"));

    // stats have updated so we should save
    actualFlushMethod.getValue().run();
    verify(attemptApi).saveStats(any());
    clearInvocations(stateApi, attemptApi);

    // stats have NOT updated so we should not save
    actualFlushMethod.getValue().run();
    verify(attemptApi, never()).saveStats(any());
    clearInvocations(stateApi, attemptApi);

    // update stats for original stream
    syncPersistence.updateStats(new AirbyteRecordMessage().withStream("stream1"));

    // stats have updated so we should save
    actualFlushMethod.getValue().run();
    verify(attemptApi).saveStats(any());
    clearInvocations(stateApi, attemptApi);
  }

  @Test
  void testClose() throws Exception {
    // Adding a state to flush, this state should get flushed when we close syncPersistence
    final AirbyteStateMessage stateA2 = getStreamState("A", 2);
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, stateA2);

    // Shutdown, we expect the executor service to be stopped and an stateApi to be called
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    syncPersistence.close();
    verify(executorService).shutdown();
    verifyStateUpdateApiCall(List.of(stateA2));
    verify(attemptApi).saveStats(any());
  }

  @Test
  void testCloseMergeStatesFromPreviousFailure() throws Exception {
    // Adding a state to flush, this state should get flushed when we close syncPersistence
    final AirbyteStateMessage stateA2 = getStreamState("closeA", 2);
    syncPersistence.accept(connectionId, stateA2);

    // Trigger a failure
    when(stateApi.createOrUpdateState(any())).thenThrow(new IOException());
    actualFlushMethod.getValue().run();

    final AirbyteStateMessage stateB1 = getStreamState("closeB", 1);
    syncPersistence.accept(connectionId, stateB1);

    // Final flush
    reset(stateApi);
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    syncPersistence.close();
    verifyStateUpdateApiCall(List.of(stateA2, stateB1));
  }

  @Test
  void testCloseShouldAttemptToRetryFinalFlush() throws Exception {
    final AirbyteStateMessage state = getStreamState("final retry", 2);
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, state);

    when(stateApi.createOrUpdateState(any()))
        .thenReturn(mock(ConnectionState.class));

    // Final flush
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    syncPersistence.close();
    verify(stateApi, times(1)).createOrUpdateState(buildStateRequest(connectionId, List.of(state)));
    verify(attemptApi, times(1)).saveStats(any());
  }

  @Test
  void testBadFinalStateFlushThrowsAnException() throws IOException, InterruptedException {
    // Setup some API failures
    when(stateApi.createOrUpdateState(any())).thenThrow(new IOException());

    final AirbyteStateMessage state = getStreamState("final retry", 2);
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, state);

    // Final flush
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    assertThrows(Exception.class, syncPersistence::close);
    verify(stateApi, times(1)).createOrUpdateState(buildStateRequest(connectionId, List.of(state)));
    verify(attemptApi, never()).saveStats(any());
  }

  @Test
  void testBadFinalStatsFlushThrowsAnException() throws IOException, InterruptedException {
    final AirbyteStateMessage state = getStreamState("final retry", 2);
    syncPersistence.updateStats(new AirbyteRecordMessage());
    syncPersistence.accept(connectionId, state);

    // Setup some API failures
    when(attemptApi.saveStats(any())).thenThrow(new IOException());

    // Final flush
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    assertThrows(Exception.class, syncPersistence::close);
    verify(stateApi).createOrUpdateState(buildStateRequest(connectionId, List.of(state)));
    verify(attemptApi, times(1)).saveStats(any());
  }

  @Test
  void testCloseWhenFailBecauseFlushTookTooLong() throws Exception {
    syncPersistence.accept(connectionId, getStreamState("oops", 42));

    // Simulates a flush taking too long to terminate
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(false);

    syncPersistence.close();
    verify(executorService).shutdown();
    // Since the previous write has an unknown state, we do not attempt to persist after the close
    verify(stateApi, never()).createOrUpdateState(any());
  }

  @Test
  void testCloseWhenFailBecauseThreadInterrupted() throws Exception {
    syncPersistence.accept(connectionId, getStreamState("oops", 42));

    // Simulates a flush taking too long to terminate
    when(executorService.awaitTermination(anyLong(), any())).thenThrow(new InterruptedException());

    syncPersistence.close();
    verify(executorService).shutdown();
    // Since the previous write has an unknown state, we do not attempt to persist after the close
    verify(stateApi, never()).createOrUpdateState(any());
  }

  @Test
  void testCloseWithPendingFlushShouldCallTheApi() throws Exception {
    // Shutdown, we expect the executor service to be stopped and an stateApi to be called
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    syncPersistence.close();
    verify(executorService).shutdown();
    verify(stateApi, never()).createOrUpdateState(any());
  }

  @Test
  void testPreventMixingDataFromDifferentConnections() {
    final AirbyteStateMessage message = getStreamState("stream", 5);
    syncPersistence.accept(connectionId, message);

    assertThrows(IllegalArgumentException.class, () -> syncPersistence.accept(UUID.randomUUID(), message));
  }

  @Test
  void testLegacyStatesAreGettingIntoTheScheduledFlushLogic() throws Exception {
    final ArgumentCaptor<ConnectionStateCreateOrUpdate> captor = ArgumentCaptor.forClass(ConnectionStateCreateOrUpdate.class);

    final AirbyteStateMessage message = getLegacyState("myFirstState");
    syncPersistence.accept(connectionId, message);

    verify(executorService).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    actualFlushMethod.getValue().run();
    verify(stateApi).createOrUpdateState(captor.capture());
    assertTrue(Jsons.serialize(captor.getValue()).contains("myFirstState"));
    clearInvocations(stateApi);

    final AirbyteStateMessage otherMessage1 = getLegacyState("myOtherState1");
    final AirbyteStateMessage otherMessage2 = getLegacyState("myOtherState2");
    syncPersistence.accept(connectionId, otherMessage1);
    syncPersistence.accept(connectionId, otherMessage2);
    when(executorService.awaitTermination(anyLong(), any())).thenReturn(true);
    syncPersistence.close();
    verify(stateApi).createOrUpdateState(captor.capture());
    assertTrue(Jsons.serialize(captor.getValue()).contains("myOtherState2"));
  }

  @Test
  void testSyncStatsTrackerWrapping() {
    syncStatsTracker = mock();
    syncPersistence = new SyncPersistenceImpl(airbyteApiClient, new StateAggregatorFactory(), syncStatsTracker, executorService,
        flushPeriod, new RetryWithJitterConfig(1, 1, 4),
        connectionId, jobId, attemptNumber);

    syncPersistence.updateStats(new AirbyteRecordMessage());
    verify(syncStatsTracker).updateStats(new AirbyteRecordMessage());
    clearInvocations(syncStatsTracker);

    syncPersistence.updateEstimates(new AirbyteEstimateTraceMessage());
    verify(syncStatsTracker).updateEstimates(new AirbyteEstimateTraceMessage());
    clearInvocations();

    syncPersistence.updateDestinationStateStats(new AirbyteStateMessage());
    verify(syncStatsTracker).updateDestinationStateStats(new AirbyteStateMessage());
    clearInvocations();

    syncPersistence.updateSourceStatesStats(new AirbyteStateMessage());
    verify(syncStatsTracker).updateSourceStatesStats(new AirbyteStateMessage());
    clearInvocations();

    syncPersistence.getStreamToCommittedBytes();
    verify(syncStatsTracker).getStreamToCommittedBytes();
    clearInvocations(syncStatsTracker);

    syncPersistence.getStreamToCommittedRecords();
    verify(syncStatsTracker).getStreamToCommittedRecords();
    clearInvocations(syncStatsTracker);

    syncPersistence.getStreamToEmittedRecords();
    verify(syncStatsTracker).getStreamToEmittedRecords();
    clearInvocations(syncStatsTracker);

    syncPersistence.getStreamToEstimatedRecords();
    verify(syncStatsTracker).getStreamToEstimatedRecords();
    clearInvocations(syncStatsTracker);

    syncPersistence.getStreamToEmittedBytes();
    verify(syncStatsTracker).getStreamToEmittedBytes();
    clearInvocations(syncStatsTracker);

    syncPersistence.getStreamToEstimatedBytes();
    verify(syncStatsTracker).getStreamToEstimatedBytes();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalRecordsEmitted();
    verify(syncStatsTracker).getTotalRecordsEmitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalRecordsEstimated();
    verify(syncStatsTracker).getTotalRecordsEstimated();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalBytesEmitted();
    verify(syncStatsTracker).getTotalBytesEmitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalBytesEstimated();
    verify(syncStatsTracker).getTotalBytesEstimated();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalBytesCommitted();
    verify(syncStatsTracker).getTotalBytesCommitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalRecordsCommitted();
    verify(syncStatsTracker).getTotalRecordsCommitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalSourceStateMessagesEmitted();
    verify(syncStatsTracker).getTotalSourceStateMessagesEmitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getTotalDestinationStateMessagesEmitted();
    verify(syncStatsTracker).getTotalDestinationStateMessagesEmitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getMaxSecondsToReceiveSourceStateMessage();
    verify(syncStatsTracker).getMaxSecondsToReceiveSourceStateMessage();
    clearInvocations(syncStatsTracker);

    syncPersistence.getMeanSecondsToReceiveSourceStateMessage();
    verify(syncStatsTracker).getMeanSecondsToReceiveSourceStateMessage();
    clearInvocations(syncStatsTracker);

    syncPersistence.getMaxSecondsBetweenStateMessageEmittedAndCommitted();
    verify(syncStatsTracker).getMaxSecondsBetweenStateMessageEmittedAndCommitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getMeanSecondsBetweenStateMessageEmittedAndCommitted();
    verify(syncStatsTracker).getMeanSecondsBetweenStateMessageEmittedAndCommitted();
    clearInvocations(syncStatsTracker);

    syncPersistence.getUnreliableStateTimingMetrics();
    verify(syncStatsTracker).getUnreliableStateTimingMetrics();
    clearInvocations(syncStatsTracker);
  }

  private void verifyStateUpdateApiCall(final List<AirbyteStateMessage> expectedStateMessages) {
    // Using an ArgumentCaptor because we do not have an ordering constraint on the states, so we need
    // to add an unordered collection equals
    final ArgumentCaptor<ConnectionStateCreateOrUpdate> captor = ArgumentCaptor.forClass(ConnectionStateCreateOrUpdate.class);

    try {
      verify(stateApi).createOrUpdateState(captor.capture());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final ConnectionStateCreateOrUpdate actual = captor.getValue();
    final ConnectionStateCreateOrUpdate expected = buildStateRequest(connectionId, expectedStateMessages);

    // Checking the stream states
    CollectionAssert.assertThatCollection(actual.getConnectionState().getStreamState())
        .containsExactlyInAnyOrderElementsOf(expected.getConnectionState().getStreamState());

    // Checking the rest of the payload
    assertEquals(expected.getConnectionState().getConnectionId(), actual.getConnectionState().getConnectionId());
    assertEquals(expected.getConnectionState().getState(), actual.getConnectionState().getState());
    assertEquals(expected.getConnectionState().getGlobalState(), actual.getConnectionState().getGlobalState());
    assertEquals(expected.getConnectionState().getStateType(), actual.getConnectionState().getStateType());
    assertEquals(expected.getConnectionId(), actual.getConnectionId());
  }

  private ConnectionStateCreateOrUpdate buildStateRequest(final UUID connectionId, final List<AirbyteStateMessage> stateMessages) {
    return new ConnectionStateCreateOrUpdate(
        connectionId,
        new ConnectionState(
            ConnectionStateType.STREAM,
            connectionId,
            null,
            stateMessages.stream().map(s -> new StreamState(
                new io.airbyte.api.client.model.generated.StreamDescriptor(
                    s.getStream().getStreamDescriptor().getName(),
                    s.getStream().getStreamDescriptor().getNamespace()),
                s.getStream().getStreamState())).toList(),
            null));
  }

  private AirbyteStateMessage getStreamState(final String streamName, final int stateValue) {
    return new AirbyteStateMessage().withType(STREAM)
        .withStream(
            new AirbyteStreamState()
                .withStreamDescriptor(
                    new StreamDescriptor()
                        .withName(streamName))
                .withStreamState(Jsons.jsonNode(stateValue)));
  }

  private AirbyteStateMessage getLegacyState(final String stateValue) {
    return new AirbyteStateMessage().withType(LEGACY)
        .withData(Jsons.deserialize("{\"state\":\"" + stateValue + "\"}"));
  }

}
