/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.model.generated.GetTaskQueueNameRequest;
import io.airbyte.api.client.model.generated.TaskQueueNameRead;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.workers.temporal.scheduling.activities.RouteToSyncTaskQueueActivity.RouteToSyncTaskQueueInput;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.openapitools.client.infrastructure.ClientException;

/**
 * Checkstyle :|.
 */
class RouteToSyncTaskQueueActivityTest {

  private AirbyteApiClient mAirbyteApiClient;
  private ConnectionApi mConnectionApi;
  private RouteToSyncTaskQueueActivityImpl activity;

  @BeforeEach
  public void setup() throws Exception {
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    mConnectionApi = mock(ConnectionApi.class);

    when(mAirbyteApiClient.getConnectionApi()).thenReturn(mConnectionApi);

    activity = new RouteToSyncTaskQueueActivityImpl(mAirbyteApiClient);
  }

  @ParameterizedTest
  @MethodSource("uuidJobTypesMatrix")
  void routeToSyncSuccess(final UUID connectionId, final TemporalJobType type) throws IOException {
    final ArgumentCaptor<GetTaskQueueNameRequest> req = ArgumentCaptor.forClass(GetTaskQueueNameRequest.class);

    final var expected = "a queue name";
    when(mConnectionApi.getTaskQueueName(any()))
        .thenReturn(new TaskQueueNameRead(expected));

    final var output = activity.routeToTask(new RouteToSyncTaskQueueInput(connectionId), type);

    verify(mConnectionApi).getTaskQueueName(req.capture());
    assertEquals(type.toString(), req.getValue().getTemporalJobType());
    assertEquals(connectionId, req.getValue().getConnectionId());
    assertEquals(expected, output.getTaskQueue());
  }

  @Test
  void routeToSyncNotFound() throws IOException {
    final ClientException exception = new ClientException("Not here.", HttpStatus.NOT_FOUND.getCode(), null);
    doThrow(exception)
        .when(mConnectionApi).getTaskQueueName(any());

    assertThrows(RuntimeException.class, () -> activity.routeToTask(new RouteToSyncTaskQueueInput(Fixtures.connectionId), Fixtures.syncType));
  }

  @Test
  void routeToSyncOtherHttpExceptionThrowsRetryable() throws IOException {
    final ClientException exception = new ClientException("Do better.", HttpStatus.BAD_REQUEST.getCode(), null);
    doThrow(exception)
        .when(mConnectionApi).getTaskQueueName(any());

    assertThrows(RetryableException.class, () -> activity.routeToTask(new RouteToSyncTaskQueueInput(Fixtures.connectionId), Fixtures.syncType));
  }

  static Stream<Arguments> uuidJobTypesMatrix() {
    return Arrays.stream(TemporalJobType.values()).map(v -> Arguments.of(UUID.randomUUID(), v));
  }

  static class Fixtures {

    static UUID connectionId = UUID.randomUUID();
    static TemporalJobType syncType = TemporalJobType.SYNC;

  }

}
