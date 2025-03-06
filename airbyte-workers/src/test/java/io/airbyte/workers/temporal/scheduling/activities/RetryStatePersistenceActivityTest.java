/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.airbyte.workers.helpers.RetryStateClient;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;

class RetryStatePersistenceActivityTest {

  @Mock
  private AirbyteApiClient mAirbyteApiClient;

  @Mock
  private RetryStateClient mRetryStateClient;

  @Mock
  private WorkspaceApi mWorkspaceApi;

  @BeforeEach
  public void setup() throws Exception {
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    mRetryStateClient = mock(RetryStateClient.class);
    mWorkspaceApi = mock(WorkspaceApi.class);
    when(mWorkspaceApi.getWorkspaceByConnectionId(any())).thenReturn(new WorkspaceRead(UUID.randomUUID(), UUID.randomUUID(), "name", "slug", false,
        UUID.randomUUID(), null, null, null, null, null, null, null, null, null, null, null, null, null));
    when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
  }

  @ParameterizedTest
  @ValueSource(longs = {124, 541, 12, 2, 1})
  void hydrateDelegatesToRetryStatePersistence(final long jobId) {
    final var manager = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    final RetryStatePersistenceActivityImpl activity = new RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient);
    when(mRetryStateClient.hydrateRetryState(eq(jobId), Mockito.any())).thenReturn(manager);

    final HydrateInput input = new HydrateInput(jobId, UUID.randomUUID());
    final var result = activity.hydrateRetryState(input);

    verify(mRetryStateClient, times(1)).hydrateRetryState(eq(jobId), Mockito.any());

    assertEquals(manager, result.getManager());
  }

  @ParameterizedTest
  @MethodSource("persistMatrix")
  void persistDelegatesToRetryStatePersistence(final long jobId, final UUID connectionId) throws IOException {
    final var success = true;
    final var manager = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    final RetryStatePersistenceActivityImpl activity = new RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient);
    when(mRetryStateClient.persistRetryState(jobId, connectionId, manager)).thenReturn(success);

    final PersistInput input = new PersistInput(jobId, connectionId, manager);
    final var result = activity.persistRetryState(input);

    verify(mRetryStateClient, times(1)).persistRetryState(jobId, connectionId, manager);

    assertEquals(success, result.getSuccess());
  }

  public static Stream<Arguments> persistMatrix() {
    return Stream.of(
        Arguments.of(1L, UUID.randomUUID()),
        Arguments.of(134512351235L, UUID.randomUUID()),
        Arguments.of(8L, UUID.randomUUID()),
        Arguments.of(999L, UUID.randomUUID()));
  }

}
