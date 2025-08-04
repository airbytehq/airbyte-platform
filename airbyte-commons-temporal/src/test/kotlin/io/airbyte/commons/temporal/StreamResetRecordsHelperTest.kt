/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.config.Attempt
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.persistence.job.JobPersistence
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import java.io.IOException
import java.util.UUID

/**
 * Test suite for the [StreamResetRecordsHelper] class.
 */
@ExtendWith(MockitoExtension::class)
internal class StreamResetRecordsHelperTest {
  @Mock
  private lateinit var jobPersistence: JobPersistence

  @Mock
  private lateinit var streamResetPersistence: StreamResetPersistence

  @InjectMocks
  private lateinit var streamResetRecordsHelper: StreamResetRecordsHelper

  @Test
  @Throws(IOException::class)
  fun testDeleteStreamResetRecordsForJob() {
    val streamsToDelete = listOf(StreamDescriptor().withName("streamname").withNamespace("namespace"))
    val jobMock =
      Job(
        JOB_ID,
        ConfigType.RESET_CONNECTION,
        CONNECTION_ID.toString(),
        JobConfig().withConfigType(ConfigType.RESET_CONNECTION).withResetConnection(
          JobResetConnectionConfig().withResetSourceConfiguration(ResetSourceConfiguration().withStreamsToReset(streamsToDelete)),
        ),
        mutableListOf<Attempt>(),
        JobStatus.PENDING,
        0L,
        0L,
        0L,
        true,
      )
    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(jobMock)

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID)
    Mockito.verify(streamResetPersistence).deleteStreamResets(CONNECTION_ID, streamsToDelete)
  }

  @Test
  @Throws(IOException::class)
  fun testIncorrectConfigType() {
    val jobMock =
      Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), JobConfig(), mutableListOf<Attempt>(), JobStatus.PENDING, 0L, 0L, 0L, true)

    Mockito.`when`(jobPersistence.getJob(JOB_ID)).thenReturn(jobMock)

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID)
    Mockito
      .verify(streamResetPersistence, Mockito.never())
      .deleteStreamResets(Mockito.any<UUID>(), Mockito.anyList())
  }

  @Test
  @Throws(IOException::class)
  fun testNoJobId() {
    streamResetRecordsHelper.deleteStreamResetRecordsForJob(null, CONNECTION_ID)
    Mockito.verify(jobPersistence, Mockito.never()).getJob(Mockito.anyLong())
    Mockito
      .verify(streamResetPersistence, Mockito.never())
      .deleteStreamResets(Mockito.any<UUID>(), Mockito.anyList())
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val JOB_ID = "123".toLong()
  }
}
