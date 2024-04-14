/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.State;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.models.JobInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenerateInputActivityImplTest {

  @Mock
  private JobsApi mJobsApi;

  @Mock
  private PayloadChecker mPayloadChecker;

  private GenerateInputActivity activity;

  @BeforeEach
  void setUp() {
    activity = new GenerateInputActivityImpl(mJobsApi, mPayloadChecker);
  }

  @Test
  void nullsOutStateAndCatalog() throws Exception {
    final var syncInput = new StandardSyncInput()
        .withCatalog(new ConfiguredAirbyteCatalog())
        .withState(new State());
    final var jobInput = new JobInput();
    jobInput.setSyncInput(syncInput);

    when(mJobsApi.getJobInput(any()))
        .thenReturn(jobInput);

    // mock this to just return the input back to us
    when(mPayloadChecker.validatePayloadSize(any(), any()))
        .thenAnswer(args -> args.getArguments()[0]);

    final var result = activity.getSyncWorkflowInput(new GenerateInputActivity.SyncInput());

    Assertions.assertNull(result.getSyncInput().getCatalog());
    Assertions.assertNull(result.getSyncInput().getState());
  }

}
