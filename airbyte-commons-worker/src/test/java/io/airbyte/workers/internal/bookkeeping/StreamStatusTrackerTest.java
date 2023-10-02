/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping;

import static io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE;
import static io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.INCOMPLETE;
import static io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.RUNNING;
import static io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.StreamStatusesApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRead;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

/**
 * Test suite for the {@link StreamStatusTracker} class.
 */
@ExtendWith(MockitoExtension.class)
class StreamStatusTrackerTest {

  private static final Integer ATTEMPT = 1;
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final Long JOB_ID = 1L;
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID STREAM_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final Duration TIMESTAMP = Duration.of(12345L, ChronoUnit.MILLIS);

  private AirbyteApiClient airbyteApiClient;
  private StreamDescriptor streamDescriptor;
  private StreamStatusesApi streamStatusesApi;
  private StreamStatusTracker streamStatusTracker;

  @Captor
  private ArgumentCaptor<StreamStatusUpdateRequestBody> updateArgumentCaptor;

  @BeforeEach
  void setup() {
    streamStatusesApi = mock(StreamStatusesApi.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    streamDescriptor = new StreamDescriptor().withName("name").withNamespace("namespace");
    streamStatusTracker = new StreamStatusTracker(airbyteApiClient);
  }

  @Test
  void testLazyMdc() {
    MDC.put("foo", "bar");
    // streamStatusTracker.postConstruct();
    assertEquals(MDC.getCopyOfContextMap(), streamStatusTracker.getMdc());
  }

  @Test
  void testCurrentStatusNoStatus() {
    final StreamStatusKey streamStatusKey =
        new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(), WORKSPACE_ID, CONNECTION_ID, JOB_ID, ATTEMPT);
    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingStartedStatus(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage airbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent event = new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    final StreamStatusCreateRequestBody expected = new StreamStatusCreateRequestBody()
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.PENDING)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());
    final StreamStatusRead streamStatusRead = new StreamStatusRead()
        .attemptNumber(ATTEMPT)
        .connectionId(CONNECTION_ID)
        .id(STREAM_ID)
        .jobId(JOB_ID)
        .jobType(StreamStatusJobType.SYNC)
        .runState(StreamStatusRunState.PENDING)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(streamStatusRead);
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(event);

    assertEquals(STARTED, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(expected);
    verify(streamStatusesApi, times(0)).updateStreamStatus(any(StreamStatusUpdateRequestBody.class));
  }

  @Test
  void testTrackingRunningStatus() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusJobType.SYNC)
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.RUNNING)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);

    assertEquals(AirbyteStreamStatus.RUNNING, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @Test
  void testTrackingCompleteSourceOnly() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceCompleteAirbyteMessage, replicationContext);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(1)).updateStreamStatus(any(StreamStatusUpdateRequestBody.class));
  }

  @Test
  void testTrackingCompleteDestinationOnly() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(destinationEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(1)).updateStreamStatus(any(StreamStatusUpdateRequestBody.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingCompleteSourceAndCompleteDestination(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceCompleteAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(destinationEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingCompleteDestinationAndCompleteSource(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceCompleteAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    assertEquals(STARTED, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));

    streamStatusTracker.track(runningEvent);
    assertEquals(RUNNING, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));

    streamStatusTracker.track(destinationEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));

    streamStatusTracker.track(sourceEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getValue();
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteSourceOnly(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteDestinationOnly(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationIncompleteAirbyteMessage, replicationContext,
            incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(destinationEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteSourceAndIncompleteDestination(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationIncompleteAirbyteMessage, replicationContext,
            incompleteRunCause);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(destinationEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteDestinationAndIncompleteSource(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationIncompleteAirbyteMessage, replicationContext,
            incompleteRunCause);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(destinationEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(sourceEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteSourceAndCompleteDestination(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(destinationEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingCompleteDestinationAndIncompleteSource(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(destinationEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(sourceEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingCompleteSourceAndIncompleteDestination(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationIncompleteAirbyteMessage, replicationContext,
            incompleteRunCause);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceCompleteAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceEvent);
    assertEquals(COMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(destinationEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingIncompleteDestinationAndCompleteSource(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage destinationIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationIncompleteAirbyteMessage, replicationContext,
            incompleteRunCause);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceCompleteAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(destinationEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(sourceEvent);
    assertEquals(INCOMPLETE, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTrackingInternalIncomplete(final boolean isReset) throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent internalEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(internalEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(2)).updateStreamStatus(updateArgumentCaptor.capture());

    final StreamStatusUpdateRequestBody result = updateArgumentCaptor.getAllValues().get(updateArgumentCaptor.getAllValues().size() - 1);
    assertEquals(expected, result);
  }

  @Test
  void testTrackingOutOfOrderStartedStatus() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage airbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent event = new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    final StreamStatusCreateRequestBody expected = new StreamStatusCreateRequestBody()
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusJobType.SYNC)
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.PENDING)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(event);
    assertEquals(STARTED, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(event);
    assertEquals(STARTED, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(expected);
  }

  @Test
  void testTrackingOutOfOrderRunningStatus() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, runningAirbyteMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusJobType.SYNC)
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.RUNNING)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(runningEvent);
    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    assertEquals(AirbyteStreamStatus.RUNNING, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    streamStatusTracker.track(runningEvent);
    assertEquals(AirbyteStreamStatus.RUNNING, streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @Test
  void testTrackingOutOfOrderCompleteStatus() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage destinationStoppedAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage sourceStoppedAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, destinationStoppedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceStoppedAirbyteMessage, replicationContext);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    streamStatusTracker.track(sourceEvent);
    streamStatusTracker.track(destinationEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(0)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(0)).updateStreamStatus(any(StreamStatusUpdateRequestBody.class));
  }

  @Test
  void testTrackingOutOfOrderIncompleteStatus() throws ApiException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage destinationStoppedAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage sourceStoppedAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final ReplicationContext replicationContext =
        new ReplicationContext(false, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationAirbyteMessageEvent destinationEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, destinationStoppedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, sourceStoppedAirbyteMessage, replicationContext);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    streamStatusTracker.track(sourceEvent);
    streamStatusTracker.track(destinationEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(0)).createStreamStatus(any(StreamStatusCreateRequestBody.class));
    verify(streamStatusesApi, times(0)).updateStreamStatus(any(StreamStatusUpdateRequestBody.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionRunning(final boolean isReset) throws ApiException {
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(forceCompletionEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionPartiallyComplete(final boolean isReset) throws ApiException {
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceCompletedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, sourceCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceCompletedEvent);

    assertFalse(streamStatusTracker.getCurrentStreamStatus(streamStatusKey).isComplete());

    streamStatusTracker.track(forceCompletionEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionAlreadyIncomplete(final boolean isReset) throws ApiException {
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceIncompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.INCOMPLETE, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext);
    final var incompleteRunCause = StreamStatusIncompleteRunCause.FAILED;
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceIncompletedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, sourceIncompleteAirbyteMessage, replicationContext, incompleteRunCause);
    final ReplicationAirbyteMessageEvent destinationCompletedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.INCOMPLETE)
        .incompleteRunCause(incompleteRunCause)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceIncompletedEvent);
    streamStatusTracker.track(destinationCompletedEvent);
    streamStatusTracker.track(forceCompletionEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionAlreadyComplete(final boolean isReset) throws ApiException {
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage sourceCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage destinationCompleteAirbyteMessage = createAirbyteMessage(streamDescriptor, COMPLETE, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, runningAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent sourceCompletedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, sourceCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent destinationCompletedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, destinationCompleteAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(sourceCompletedEvent);
    streamStatusTracker.track(destinationCompletedEvent);
    streamStatusTracker.track(forceCompletionEvent);

    assertNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(1)).updateStreamStatus(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionDifferentConnectionId(final boolean isReset) throws ApiException {
    final Integer attempt = 2;
    final Long jobId = 2L;
    final UUID connectionId = UUID.randomUUID();
    final ReplicationContext replicationContext1 =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);
    final ReplicationContext replicationContext2 =
        new ReplicationContext(isReset, connectionId, UUID.randomUUID(), UUID.randomUUID(), jobId, attempt, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage runningAirbyteMessage = createAirbyteMessage(streamDescriptor, AirbyteStreamStatus.RUNNING, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext1);
    final ReplicationAirbyteMessageEvent runningEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, runningAirbyteMessage, replicationContext1);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext2);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(jobId)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext1))
        .connectionId(connectionId)
        .attemptNumber(attempt)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext1.workspaceId(), replicationContext1.connectionId(), replicationContext1.jobId(), replicationContext1.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead().id(STREAM_ID));
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    streamStatusTracker.track(startedEvent);
    streamStatusTracker.track(runningEvent);
    streamStatusTracker.track(forceCompletionEvent);

    assertNotNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
    verify(streamStatusesApi, times(0)).updateStreamStatus(expected);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testForceCompletionHandleException(final boolean isReset) throws ApiException {
    final ReplicationContext replicationContext =
        new ReplicationContext(isReset, CONNECTION_ID, DESTINATION_ID, SOURCE_ID, JOB_ID, ATTEMPT, WORKSPACE_ID);

    final AirbyteMessage startedAirbyteMessage = createAirbyteMessage(streamDescriptor, STARTED, TIMESTAMP);
    final AirbyteMessage forceCompletionMessage = createAirbyteMessage(new StreamDescriptor(), COMPLETE, TIMESTAMP);

    final ReplicationAirbyteMessageEvent startedEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, startedAirbyteMessage, replicationContext);
    final ReplicationAirbyteMessageEvent forceCompletionEvent =
        new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.INTERNAL, forceCompletionMessage, replicationContext);
    final StreamStatusUpdateRequestBody expected = new StreamStatusUpdateRequestBody()
        .id(STREAM_ID)
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(JOB_ID)
        .jobType(StreamStatusTrackerKt.jobType(replicationContext))
        .connectionId(CONNECTION_ID)
        .attemptNumber(ATTEMPT)
        .runState(StreamStatusRunState.COMPLETE)
        .transitionedAt(TIMESTAMP.toMillis())
        .workspaceId(WORKSPACE_ID);
    final StreamStatusKey streamStatusKey = new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(),
        replicationContext.workspaceId(), replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());

    when(streamStatusesApi.createStreamStatus(any())).thenReturn(new StreamStatusRead());
    when(airbyteApiClient.getStreamStatusesApi()).thenReturn(streamStatusesApi);

    assertDoesNotThrow(() -> {
      streamStatusTracker.track(startedEvent);
      streamStatusTracker.track(forceCompletionEvent);
      assertNotNull(streamStatusTracker.getAirbyteStreamStatus(streamStatusKey));
      verify(streamStatusesApi, times(0)).updateStreamStatus(expected);
    });
  }

  private AirbyteMessage createAirbyteMessage(final StreamDescriptor streamDescriptor, final AirbyteStreamStatus status, final Duration timestamp) {
    final AirbyteStreamStatusTraceMessage statusTraceMessage =
        new AirbyteStreamStatusTraceMessage().withStreamDescriptor(streamDescriptor).withStatus(status);
    final AirbyteTraceMessage traceMessage = new AirbyteTraceMessage().withType(AirbyteTraceMessage.Type.STREAM_STATUS)
        .withStreamStatus(statusTraceMessage).withEmittedAt(Long.valueOf(timestamp.toMillis()).doubleValue());
    return new AirbyteMessage().withType(Type.TRACE).withTrace(traceMessage);
  }

}
