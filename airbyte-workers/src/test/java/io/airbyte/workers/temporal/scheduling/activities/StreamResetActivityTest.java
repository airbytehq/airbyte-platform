/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput;
import java.util.UUID;
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
  private JobsApi jobsApi;
  @InjectMocks
  private StreamResetActivityImpl streamResetActivity;

  @Test
  void deleteStreamResetRecordsForJobSuccess() throws ApiException {
    final DeleteStreamResetRecordsForJobInput input = new DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), Long.valueOf("123"));

    final ArgumentCaptor<DeleteStreamResetRecordsForJobRequest> req = ArgumentCaptor.forClass(DeleteStreamResetRecordsForJobRequest.class);

    streamResetActivity.deleteStreamResetRecordsForJob(input);

    verify(jobsApi).deleteStreamResetRecordsForJob(req.capture());
    assertEquals(input.getJobId(), req.getValue().getJobId());
    assertEquals(input.getConnectionId(), req.getValue().getConnectionId());
  }

  @Test
  void deleteStreamResetRecordsForJobThrowsRetryableException() throws ApiException {
    final DeleteStreamResetRecordsForJobInput input = new DeleteStreamResetRecordsForJobInput(UUID.randomUUID(), Long.valueOf("123"));

    Mockito.doThrow(new ApiException("bang.")).when(jobsApi).deleteStreamResetRecordsForJob(any());

    assertThrows(RetryableException.class, () -> streamResetActivity.deleteStreamResetRecordsForJob(input));
  }

}
