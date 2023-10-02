/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.GetTaskQueueNameRequest;
import io.airbyte.api.client.model.generated.TaskQueueNameRead;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.workers.temporal.scheduling.activities.RouteToSyncTaskQueueActivity.RouteToSyncTaskQueueInput;
import io.micronaut.http.HttpStatus;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Checkstyle :|.
 */
class RouteToSyncTaskQueueActivityTest {

  private ConnectionApi mConnectionApi;
  private RouteToSyncTaskQueueActivityImpl activity;

  @BeforeEach
  public void setup() throws Exception {
    mConnectionApi = mock(ConnectionApi.class);

    activity = new RouteToSyncTaskQueueActivityImpl(mConnectionApi);
  }

  @ParameterizedTest
  @MethodSource("uuidJobTypesMatrix")
  void routeToSyncSuccess(final UUID connectionId, final TemporalJobType type) throws ApiException {
    final ArgumentCaptor<GetTaskQueueNameRequest> req = ArgumentCaptor.forClass(GetTaskQueueNameRequest.class);

    final var expected = "a queue name";
    when(mConnectionApi.getTaskQueueName(any()))
        .thenReturn(new TaskQueueNameRead().taskQueueName(expected));

    final var output = activity.routeToTask(new RouteToSyncTaskQueueInput(connectionId), type);

    verify(mConnectionApi).getTaskQueueName(req.capture());
    assertEquals(type.toString(), req.getValue().getTemporalJobType());
    assertEquals(connectionId, req.getValue().getConnectionId());
    assertEquals(expected, output.getTaskQueue());
  }

  @Test
  void routeToSyncNotFound() throws ApiException {
    final ApiException exception = new ApiException(HttpStatus.NOT_FOUND.getCode(), "Not here.");
    doThrow(exception)
        .when(mConnectionApi).getTaskQueueName(any());

    assertThrows(RuntimeException.class, () -> activity.routeToTask(new RouteToSyncTaskQueueInput(Fixtures.connectionId), Fixtures.syncType));
  }

  @Test
  void routeToSyncOtherHttpExceptionThrowsRetryable() throws ApiException {
    final ApiException exception = new ApiException(HttpStatus.BAD_REQUEST.getCode(), "Do better.");
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
