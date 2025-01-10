/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobStatus;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.SyncStats;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TrackingMetadataTest {

  private static final String UNUSED = "unused";

  @Test
  void testNulls() {
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final StandardSync standardSync = mock(StandardSync.class);

    // set all the required values for a valid connection
    when(standardSync.getConnectionId()).thenReturn(connectionId);
    when(standardSync.getName()).thenReturn("connection-name");
    when(standardSync.getManual()).thenReturn(true);
    when(standardSync.getSourceId()).thenReturn(sourceId);
    when(standardSync.getDestinationId()).thenReturn(destinationId);
    when(standardSync.getCatalog()).thenReturn(mock(ConfiguredAirbyteCatalog.class));
    when(standardSync.getResourceRequirements()).thenReturn(new ResourceRequirements());

    // make sure to use a null for resources
    when(standardSync.getCatalog()).thenReturn(mock(ConfiguredAirbyteCatalog.class));

    // try to generate metadata
    final Map<String, Object> expected = Map.of(
        "connection_id", connectionId,
        "frequency", "manual",
        "operation_count", 0,
        "table_prefix", false,
        "source_id", sourceId,
        "destination_id", destinationId);
    final Map<String, Object> actual = TrackingMetadata.generateSyncMetadata(standardSync);
    assertEquals(expected, actual);
  }

  @Test
  void testgenerateJobAttemptMetadataWithNulls() {
    final SyncStats syncStats = new SyncStats().withRecordsCommitted(10L).withRecordsEmitted(10L).withBytesEmitted(100L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(5L).withMaxSecondsBeforeSourceStateMessageEmitted(8L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L).withMaxSecondsBetweenStateMessageEmittedandCommitted(null);
    final StandardSyncSummary standardSyncSummary = new StandardSyncSummary().withTotalStats(syncStats);
    final StandardSyncOutput standardSyncOutput = new StandardSyncOutput().withStandardSyncSummary(standardSyncSummary);
    final AttemptSyncConfig attemptSyncConfig = mock(AttemptSyncConfig.class);
    final JobOutput jobOutput = new JobOutput().withSync(standardSyncOutput);
    final Attempt attempt = new Attempt(0, 10L, Path.of("test"), attemptSyncConfig, jobOutput, AttemptStatus.SUCCEEDED, null, null, 100L, 100L, 99L);
    final Job job = mock(Job.class);
    when(job.getAttempts()).thenReturn(List.of(attempt));

    final Map<String, Object> actual = TrackingMetadata.generateJobAttemptMetadata(job);
    final Map<String, Object> expected = Map.of(
        "mean_seconds_before_source_state_message_emitted", 2L,
        "mean_seconds_between_state_message_emit_and_commit", 5L,
        "max_seconds_before_source_state_message_emitted", 8L);
    assertEquals(expected, actual);
  }

  @Test
  void testGenerateJobAttemptMetadataToleratesNullInputs() {
    // Null job.
    assertTrue(TrackingMetadata.generateJobAttemptMetadata(null).isEmpty());

    // There is a job, but it has no attempts.
    final Job jobWithNoAttempts = new Job(1, JobConfig.ConfigType.SYNC, UNUSED, null, List.of(), JobStatus.PENDING, 0L, 0, 0);
    assertTrue(TrackingMetadata.generateJobAttemptMetadata(jobWithNoAttempts).isEmpty());

    // There is a job, and it has an attempt, but the attempt has null output.
    final Attempt mockAttemptWithNullOutput = mock(Attempt.class);
    when(mockAttemptWithNullOutput.getOutput()).thenReturn(null);
    final Job jobWithNullOutput =
        new Job(1, JobConfig.ConfigType.SYNC, UNUSED, null, List.of(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0);
    assertTrue(TrackingMetadata.generateJobAttemptMetadata(jobWithNullOutput).isEmpty());

    // There is a job, and it has an attempt, but the attempt has empty output.
    final Attempt mockAttemptWithEmptyOutput = mock(Attempt.class);
    when(mockAttemptWithEmptyOutput.getOutput()).thenReturn(Optional.empty());
    final Job jobWithEmptyOutput =
        new Job(1, JobConfig.ConfigType.SYNC, UNUSED, null, List.of(mockAttemptWithNullOutput), JobStatus.PENDING, 0L, 0, 0);
    assertTrue(TrackingMetadata.generateJobAttemptMetadata(jobWithEmptyOutput).isEmpty());

    // There is a job, and it has an attempt, and the attempt has output, but the output has no sync
    // info.
    final Attempt mockAttemptWithOutput = mock(Attempt.class);
    final JobOutput mockJobOutputWithoutSync = mock(JobOutput.class);
    when(mockAttemptWithOutput.getOutput()).thenReturn(Optional.of(mockJobOutputWithoutSync));
    when(mockJobOutputWithoutSync.getSync()).thenReturn(null);
    final Job jobWithoutSyncInfo = new Job(1, JobConfig.ConfigType.SYNC, UNUSED, null, List.of(mockAttemptWithOutput), JobStatus.PENDING, 0L, 0, 0);
    assertTrue(TrackingMetadata.generateJobAttemptMetadata(jobWithoutSyncInfo).isEmpty());
  }

}
