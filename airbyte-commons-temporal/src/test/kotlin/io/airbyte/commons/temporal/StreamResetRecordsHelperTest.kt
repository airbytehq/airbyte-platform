/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.persistence.job.JobPersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * Test suite for the [StreamResetRecordsHelper] class.
 */
internal class StreamResetRecordsHelperTest {
  private lateinit var jobPersistence: JobPersistence
  private lateinit var streamResetPersistence: StreamResetPersistence
  private lateinit var streamResetRecordsHelper: StreamResetRecordsHelper

  @BeforeEach
  fun setup() {
    jobPersistence = mockk()
    streamResetPersistence = mockk(relaxed = true)
    streamResetRecordsHelper = StreamResetRecordsHelper(jobPersistence, streamResetPersistence)
  }

  @Test
  fun testDeleteStreamResetRecordsForJob() {
    val streamsToDelete = listOf(StreamDescriptor().withName("stream-name").withNamespace("namespace"))
    val jobMock =
      Job(
        JOB_ID,
        ConfigType.RESET_CONNECTION,
        CONNECTION_ID.toString(),
        JobConfig().withConfigType(ConfigType.RESET_CONNECTION).withResetConnection(
          JobResetConnectionConfig().withResetSourceConfiguration(ResetSourceConfiguration().withStreamsToReset(streamsToDelete)),
        ),
        mutableListOf(),
        JobStatus.PENDING,
        0L,
        0L,
        0L,
        true,
      )
    every { jobPersistence.getJob(JOB_ID) } returns jobMock

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID)
    verify { streamResetPersistence.deleteStreamResets(CONNECTION_ID, streamsToDelete) }
  }

  @Test
  fun testIncorrectConfigType() {
    val jobMock =
      Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), JobConfig(), mutableListOf(), JobStatus.PENDING, 0L, 0L, 0L, true)

    every { jobPersistence.getJob(JOB_ID) } returns jobMock

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID)
    verify(exactly = 0) { streamResetPersistence.deleteStreamResets(any<UUID>(), any<List<StreamDescriptor>>()) }
  }

  @Test
  fun testNoJobId() {
    streamResetRecordsHelper.deleteStreamResetRecordsForJob(null, CONNECTION_ID)
    verify(exactly = 0) { jobPersistence.getJob(any<Long>()) }
    verify(exactly = 0) { streamResetPersistence.deleteStreamResets(any<UUID>(), any<List<StreamDescriptor>>()) }
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val JOB_ID = "123".toLong()
  }
}
