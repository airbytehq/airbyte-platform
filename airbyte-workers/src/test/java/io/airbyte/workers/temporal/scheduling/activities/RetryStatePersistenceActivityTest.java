/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseNewRetries;
import io.airbyte.workers.helpers.RetryStateClient;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput;
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
  private RetryStateClient mRetryStateClient;

  @Mock
  private FeatureFlagClient mFeatureFlagClient;

  @Mock
  private WorkspaceApi mWorkspaceApi;

  @BeforeEach
  public void setup() throws Exception {
    mRetryStateClient = Mockito.mock(RetryStateClient.class);
    mFeatureFlagClient = Mockito.mock(TestClient.class);
    mWorkspaceApi = Mockito.mock(WorkspaceApi.class);

    when(mWorkspaceApi.getWorkspaceByConnectionId(any())).thenReturn(new WorkspaceRead().workspaceId(UUID.randomUUID()));
  }

  @ParameterizedTest
  @ValueSource(longs = {124, 541, 12, 2, 1})
  void hydrateDelegatesToRetryStatePersistence(final long jobId) {
    final var manager = RetryManager.builder().build();
    final RetryStatePersistenceActivityImpl activity = new RetryStatePersistenceActivityImpl(mRetryStateClient, mFeatureFlagClient, mWorkspaceApi);
    when(mRetryStateClient.hydrateRetryState(eq(jobId), Mockito.any())).thenReturn(manager);
    when(mFeatureFlagClient.boolVariation(eq(UseNewRetries.INSTANCE), any())).thenReturn(true);

    final HydrateInput input = new HydrateInput(jobId, UUID.randomUUID());
    final var result = activity.hydrateRetryState(input);

    verify(mRetryStateClient, times(1)).hydrateRetryState(eq(jobId), Mockito.any());

    assertEquals(manager, result.getManager());
  }

  @ParameterizedTest
  @ValueSource(longs = {124, 541, 12, 2, 1})
  void returnsNullWhenFFOff(final long jobId) {
    final var manager = RetryManager.builder().build();
    final RetryStatePersistenceActivityImpl activity = new RetryStatePersistenceActivityImpl(mRetryStateClient, mFeatureFlagClient, mWorkspaceApi);
    when(mRetryStateClient.hydrateRetryState(eq(jobId), Mockito.any())).thenReturn(manager);
    when(mFeatureFlagClient.boolVariation(eq(UseNewRetries.INSTANCE), any())).thenReturn(false);

    final HydrateInput input = new HydrateInput(jobId, UUID.randomUUID());
    final var result = activity.hydrateRetryState(input);

    verify(mRetryStateClient, times(0)).hydrateRetryState(eq(jobId), Mockito.any());

    assertNull(result.getManager());
  }

  @ParameterizedTest
  @MethodSource("persistMatrix")
  void persistDelegatesToRetryStatePersistence(final long jobId, final UUID connectionId) {
    final var success = true;
    final var manager = RetryManager.builder().build();
    final RetryStatePersistenceActivityImpl activity = new RetryStatePersistenceActivityImpl(mRetryStateClient, mFeatureFlagClient, mWorkspaceApi);
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
