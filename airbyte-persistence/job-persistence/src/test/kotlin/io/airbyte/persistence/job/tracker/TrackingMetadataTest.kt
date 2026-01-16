/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

internal class TrackingMetadataTest {
  @Test
  fun testNulls() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val standardSync = mockk<StandardSync>(relaxed = true)

    // set all the required values for a valid connection
    every { standardSync.connectionId } returns connectionId
    every { standardSync.name } returns "connection-name"
    every { standardSync.manual } returns true
    every { standardSync.sourceId } returns sourceId
    every { standardSync.destinationId } returns destinationId
    every { standardSync.catalog } returns mockk<ConfiguredAirbyteCatalog>()
    every { standardSync.resourceRequirements } returns ResourceRequirements()
    every { standardSync.namespaceDefinition } returns null
    every { standardSync.prefix } returns null
    every { standardSync.operationIds } returns null

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
    val attemptSyncConfig = mockk<AttemptSyncConfig>()
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
    val mockAttemptWithNullOutput = mockk<Attempt>()
    every { mockAttemptWithNullOutput.output } returns null
    val jobWithNullOutput =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithNullOutput).isEmpty())

    // There is a job, and it has an attempt, but the attempt has empty output.
    val mockAttemptWithEmptyOutput = mockk<Attempt>()
    every { mockAttemptWithEmptyOutput.output } returns null
    val jobWithEmptyOutput =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithEmptyOutput).isEmpty())

    // There is a job, and it has an attempt, and the attempt has output, but the output has no sync
    // info.
    val mockAttemptWithOutput = mockk<Attempt>()
    val mockJobOutputWithoutSync = mockk<JobOutput>()
    every { mockAttemptWithOutput.output } returns mockJobOutputWithoutSync
    every { mockJobOutputWithoutSync.sync } returns null

    val jobWithoutSyncInfo =
      Job(1, ConfigType.SYNC, UNUSED, JobConfig(), listOf(mockAttemptWithOutput), JobStatus.PENDING, 0L, 0, 0, true)
    assertTrue(generateJobAttemptMetadata(jobWithoutSyncInfo).isEmpty())
  }

  companion object {
    private const val UNUSED = "unused"
  }
}
