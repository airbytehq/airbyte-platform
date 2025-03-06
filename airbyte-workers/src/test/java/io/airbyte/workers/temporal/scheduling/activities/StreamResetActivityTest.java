/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamResetActivityTest {

  @Mock
  private AirbyteApiClient airbyteApiClient;
  @Mock
  private JobsApi jobsApi;
  @InjectMocks
  private StreamResetActivityImpl streamResetActivity;

  @BeforeEach
  void setup() {
    when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
  }

  @Test
  void deleteStreamResetRecordsForJobSuccess() throws IOException {
    final DeleteStreamResetRecordsForJobInput input = new DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), Long.valueOf("123"));

    final ArgumentCaptor<DeleteStreamResetRecordsForJobRequest> req = ArgumentCaptor.forClass(DeleteStreamResetRecordsForJobRequest.class);

    streamResetActivity.deleteStreamResetRecordsForJob(input);

    verify(jobsApi).deleteStreamResetRecordsForJob(req.capture());
    assertEquals(input.getJobId(), req.getValue().getJobId());
    assertEquals(input.getConnectionId(), req.getValue().getConnectionId());
  }

  @Test
  void deleteStreamResetRecordsForJobThrowsRetryableException() throws IOException {
    final DeleteStreamResetRecordsForJobInput input = new DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), Long.valueOf("123"));

    Mockito.doThrow(new IOException("bang.")).when(jobsApi).deleteStreamResetRecordsForJob(any());

    assertThrows(RetryableException.class, () -> streamResetActivity.deleteStreamResetRecordsForJob(input));
  }

}
