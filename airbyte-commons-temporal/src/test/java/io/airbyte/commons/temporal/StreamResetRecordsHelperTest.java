/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.persistence.job.JobPersistence;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test suite for the {@link StreamResetRecordsHelper} class.
 */
@ExtendWith(MockitoExtension.class)
class StreamResetRecordsHelperTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final Long JOB_ID = Long.valueOf("123");

  @Mock
  private JobPersistence jobPersistence;
  @Mock
  private StreamResetPersistence streamResetPersistence;
  @InjectMocks
  private StreamResetRecordsHelper streamResetRecordsHelper;

  @Test
  void testDeleteStreamResetRecordsForJob() throws IOException {
    final List<StreamDescriptor> streamsToDelete = List.of(new StreamDescriptor().withName("streamname").withNamespace("namespace"));
    final Job jobMock = new Job(JOB_ID, ConfigType.RESET_CONNECTION, CONNECTION_ID.toString(),
        new JobConfig().withConfigType(ConfigType.RESET_CONNECTION).withResetConnection(
            new JobResetConnectionConfig().withResetSourceConfiguration(new ResetSourceConfiguration().withStreamsToReset(streamsToDelete))),
        Collections.emptyList(), JobStatus.PENDING, 0L, 0L, 0L, true);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(jobMock);

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID);
    Mockito.verify(streamResetPersistence).deleteStreamResets(CONNECTION_ID, streamsToDelete);
  }

  @Test
  void testIncorrectConfigType() throws IOException {
    final Job jobMock =
        new Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), new JobConfig(), Collections.emptyList(), JobStatus.PENDING, 0L, 0L, 0L, true);

    when(jobPersistence.getJob(JOB_ID)).thenReturn(jobMock);

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID);
    Mockito.verify(streamResetPersistence, never()).deleteStreamResets(Mockito.any(UUID.class), Mockito.anyList());
  }

  @Test
  void testNoJobId() throws IOException {
    streamResetRecordsHelper.deleteStreamResetRecordsForJob(null, CONNECTION_ID);
    Mockito.verify(jobPersistence, never()).getJob(Mockito.anyLong());
    Mockito.verify(streamResetPersistence, never()).deleteStreamResets(Mockito.any(UUID.class), Mockito.anyList());
  }

}
