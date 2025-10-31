/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import io.airbyte.config.Attempt
import io.airbyte.config.AttemptStatus
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.persistence.job.tracker.TrackingMetadata.generateJobAttemptMetadata
import io.airbyte.persistence.job.tracker.TrackingMetadata.generateSyncMetadata
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

internal class TrackingMetadataTest {
  @Test
  fun testNulls() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val standardSync = mock<StandardSync>()

    // set all the required values for a valid connection
    whenever(standardSync.connectionId).thenReturn(connectionId)
    whenever(standardSync.name).thenReturn("connection-name")
    whenever(standardSync.manual).thenReturn(true)
    whenever(standardSync.sourceId).thenReturn(sourceId)
    whenever(standardSync.destinationId).thenReturn(destinationId)
    whenever(standardSync.catalog).thenReturn(
      mock<ConfiguredAirbyteCatalog>(),
    )
    whenever(standardSync.resourceRequirements).thenReturn(ResourceRequirements())

    // make sure to use a null for resources
    whenever(standardSync.catalog).thenReturn(
      mock<ConfiguredAirbyteCatalog>(),
    )

    // try to generate metadata
    val expected =
      mapOf(
        "connection_id" to connectionId,
        "frequency" to "manual",
        "operation_count" to 0,
        "table_prefix" to false,
        "source_id" to sourceId,
        "destination_id" to destinationId,
      )
    val actual = generateSyncMetadata(standardSync)
    assertEquals(expected, actual)
  }

  @Test
  fun testGenerateJobAttemptMetadataWithNulls() {
    val syncStats =
      SyncStats()
        .withRecordsCommitted(10L)
        .withRecordsEmitted(10L)
        .withBytesEmitted(100L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(5L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(8L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(null)
    val standardSyncSummary = StandardSyncSummary().withTotalStats(syncStats)
    val standardSyncOutput = StandardSyncOutput().withStandardSyncSummary(standardSyncSummary)
    val attemptSyncConfig = mock<AttemptSyncConfig>()
    val jobOutput = JobOutput().withSync(standardSyncOutput)
    val attempt = Attempt(0, 10L, Path.of("test"), attemptSyncConfig, jobOutput, AttemptStatus.SUCCEEDED, null, null, 100L, 100L, 99L)
    val job = Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(attempt), JobStatus.PENDING, 0L, 0, 0, true)

    val actual = generateJobAttemptMetadata(job)
    val expected =
      mapOf(
        "mean_seconds_before_source_state_message_emitted" to 2L,
        "mean_seconds_between_state_message_emit_and_commit" to 5L,
        "max_seconds_before_source_state_message_emitted" to 8L,
      )
    assertEquals(expected, actual)
  }

  @Test
  fun testGenerateJobAttemptMetadataToleratesNullInputs() {
    // Null job.
    assertTrue(generateJobAttemptMetadata(null).isEmpty())

    // There is a job, but it has no attempts.
    val jobWithNoAttempts = Job(1, ConfigType.SYNC, UNUSED, JobConfig(), mutableListOf(), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithNoAttempts).isEmpty())

    // There is a job, and it has an attempt, but the attempt has null output.
    val mockAttemptWithNullOutput = mock<Attempt>()
    whenever(mockAttemptWithNullOutput.output).thenReturn(null)
    val jobWithNullOutput =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithNullOutput).isEmpty())

    // There is a job, and it has an attempt, but the attempt has empty output.
    val mockAttemptWithEmptyOutput = mock<Attempt>()
    whenever(mockAttemptWithEmptyOutput.output).thenReturn(null)
    val jobWithEmptyOutput =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithEmptyOutput).isEmpty())

    // There is a job, and it has an attempt, and the attempt has output, but the output has no sync
    // info.
    val mockAttemptWithOutput = mock<Attempt>()
    val mockJobOutputWithoutSync = mock<JobOutput>()
    whenever(mockAttemptWithOutput.output).thenReturn(mockJobOutputWithoutSync)
    whenever(mockJobOutputWithoutSync.sync).thenReturn(null)

    val jobWithoutSyncInfo =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithoutSyncInfo).isEmpty())
  }

  companion object {
    private const val UNUSED = "unused"
  }
}
