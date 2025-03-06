/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenerateInputActivityImplTest {

  @Mock
  private AirbyteApiClient mAirbyteApiClient;

  @Mock
  private JobsApi mJobsApi;

  @Mock
  private PayloadChecker mPayloadChecker;

  private GenerateInputActivity activity;

  @BeforeEach
  void setUp() {
    when(mAirbyteApiClient.getJobsApi()).thenReturn(mJobsApi);
    activity = new GenerateInputActivityImpl(mAirbyteApiClient, mPayloadChecker);
  }

}
