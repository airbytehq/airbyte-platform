/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.AttemptSyncConfig;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.model.generated.GlobalState;
import io.airbyte.api.model.generated.LogRead;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SetWorkflowInAttemptRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnprocessableContentException;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.AttemptStatus;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class AttemptHandlerTest {

  JobConverter jobConverter;
  JobPersistence jobPersistence;
  StatePersistence statePersistence;
  Path path;
  AttemptHandler handler;
  JobCreationAndStatusUpdateHelper helper;
  FeatureFlagClient ffClient;

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final long JOB_ID = 10002L;
  private static final int ATTEMPT_NUMBER = 1;
  private static final String PROCESSING_TASK_QUEUE = "SYNC";

  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED))
      .withNormalizationSummary(
          new NormalizationSummary());

  private static final JobOutput jobOutput = new JobOutput().withSync(standardSyncOutput);

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

  @BeforeEach
  public void init() {
    jobPersistence = mock(JobPersistence.class);
    statePersistence = mock(StatePersistence.class);
    jobConverter = mock(JobConverter.class);
    path = mock(Path.class);
    helper = mock(JobCreationAndStatusUpdateHelper.class);
    ffClient = mock(TestClient.class);

    handler = new AttemptHandler(jobPersistence, statePersistence, jobConverter, ffClient, helper, path);
  }

  @Test
  void testInternalWorkerHandlerSetsTemporalWorkflowId() throws Exception {
    final String workflowId = UUID.randomUUID().toString();

    final ArgumentCaptor<Integer> attemptNumberCapture = ArgumentCaptor.forClass(Integer.class);
    final ArgumentCaptor<Long> jobIdCapture = ArgumentCaptor.forClass(Long.class);
    final ArgumentCaptor<String> workflowIdCapture = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> queueCapture = ArgumentCaptor.forClass(String.class);

    final SetWorkflowInAttemptRequestBody requestBody =
        new SetWorkflowInAttemptRequestBody().attemptNumber(ATTEMPT_NUMBER).jobId(JOB_ID).workflowId(workflowId)
            .processingTaskQueue(PROCESSING_TASK_QUEUE);

    assertTrue(handler.setWorkflowInAttempt(requestBody).getSucceeded());

    verify(jobPersistence).setAttemptTemporalWorkflowInfo(jobIdCapture.capture(), attemptNumberCapture.capture(), workflowIdCapture.capture(),
        queueCapture.capture());

    assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.getValue());
    assertEquals(JOB_ID, jobIdCapture.getValue());
    assertEquals(workflowId, workflowIdCapture.getValue());
    assertEquals(PROCESSING_TASK_QUEUE, queueCapture.getValue());
  }

  @Test
  void testInternalWorkerHandlerSetsTemporalWorkflowIdThrows() throws Exception {
    final String workflowId = UUID.randomUUID().toString();

    doThrow(IOException.class).when(jobPersistence).setAttemptTemporalWorkflowInfo(anyLong(), anyInt(),
        any(), any());

    final ArgumentCaptor<Integer> attemptNumberCapture = ArgumentCaptor.forClass(Integer.class);
    final ArgumentCaptor<Long> jobIdCapture = ArgumentCaptor.forClass(Long.class);
    final ArgumentCaptor<String> workflowIdCapture = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> queueCapture = ArgumentCaptor.forClass(String.class);

    final SetWorkflowInAttemptRequestBody requestBody =
        new SetWorkflowInAttemptRequestBody().attemptNumber(ATTEMPT_NUMBER).jobId(JOB_ID).workflowId(workflowId)
            .processingTaskQueue(PROCESSING_TASK_QUEUE);

    assertFalse(handler.setWorkflowInAttempt(requestBody).getSucceeded());

    verify(jobPersistence).setAttemptTemporalWorkflowInfo(jobIdCapture.capture(), attemptNumberCapture.capture(), workflowIdCapture.capture(),
        queueCapture.capture());

    assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.getValue());
    assertEquals(JOB_ID, jobIdCapture.getValue());
    assertEquals(workflowId, workflowIdCapture.getValue());
    assertEquals(PROCESSING_TASK_QUEUE, queueCapture.getValue());
  }

  @Test
  @SuppressWarnings("PMD")
  void testInternalHandlerSetsAttemptSyncConfig() throws Exception {
    final ArgumentCaptor<Integer> attemptNumberCapture = ArgumentCaptor.forClass(Integer.class);
    final ArgumentCaptor<Long> jobIdCapture = ArgumentCaptor.forClass(Long.class);
    final ArgumentCaptor<io.airbyte.config.AttemptSyncConfig> attemptSyncConfigCapture =
        ArgumentCaptor.forClass(io.airbyte.config.AttemptSyncConfig.class);

    final JsonNode sourceConfig = Jsons.jsonNode(Map.of("source_key", "source_val"));
    final JsonNode destinationConfig = Jsons.jsonNode(Map.of("destination_key", "destination_val"));
    final ConnectionState state = new ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.GLOBAL)
        .streamState(null)
        .globalState(new GlobalState().sharedState(Jsons.jsonNode(Map.of("state_key", "state_val"))));

    final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig()
        .destinationConfiguration(destinationConfig)
        .sourceConfiguration(sourceConfig)
        .state(state);

    final SaveAttemptSyncConfigRequestBody requestBody =
        new SaveAttemptSyncConfigRequestBody().attemptNumber(ATTEMPT_NUMBER).jobId(JOB_ID).syncConfig(attemptSyncConfig);

    assertTrue(handler.saveSyncConfig(requestBody).getSucceeded());

    verify(jobPersistence).writeAttemptSyncConfig(jobIdCapture.capture(), attemptNumberCapture.capture(), attemptSyncConfigCapture.capture());

    final io.airbyte.config.AttemptSyncConfig expectedAttemptSyncConfig = ApiPojoConverters.attemptSyncConfigToInternal(attemptSyncConfig);

    assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.getValue());
    assertEquals(JOB_ID, jobIdCapture.getValue());
    assertEquals(expectedAttemptSyncConfig, attemptSyncConfigCapture.getValue());
  }

  @Test
  void createAttemptNumber() throws IOException {
    final int attemptNumber = 1;
    final var connId = UUID.randomUUID();
    final Job mJob = mock(Job.class);
    when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.SYNC);
    when(mJob.getAttemptsCount()).thenReturn(ATTEMPT_NUMBER);
    when(mJob.getScope()).thenReturn(connId.toString());

    final var mConfig = mock(JobConfig.class);
    when(mJob.getConfig()).thenReturn(mConfig);

    final var mDyncConfig = mock(JobSyncConfig.class);
    when(mConfig.getSync()).thenReturn(mDyncConfig);
    when(mDyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

    final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
    when(mDyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);

    when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);
    when(path.resolve(Mockito.anyString())).thenReturn(path);

    final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
    final Path expectedLogPath = expectedRoot.resolve(LogClientSingleton.LOG_FILENAME);

    when(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
        .thenReturn(attemptNumber);
    when(ffClient.boolVariation(any(), any())).thenReturn(true);

    final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
    assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);
  }

  @Nested
  class ClearFullRefreshStreamStateFirstAttempt {

    @Test
    void getFullRefreshStreamsShouldOnlyReturnFullRefreshStreams() {
      final var connId = UUID.randomUUID();
      final Job mJob = mock(Job.class);
      when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.SYNC);
      when(mJob.getScope()).thenReturn(connId.toString());

      final var mJobConfig = mock(JobConfig.class);
      when(mJob.getConfig()).thenReturn(mJobConfig);

      final var mSyncConfig = mock(JobSyncConfig.class);
      when(mJobConfig.getSync()).thenReturn(mSyncConfig);

      final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
      when(mSyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);

      when(mCatalog.getStreams()).thenReturn(List.of(
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.FULL_REFRESH).withStream(new AirbyteStream().withName("full")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.INCREMENTAL).withStream(new AirbyteStream().withName("incre")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.FULL_REFRESH).withStream(new AirbyteStream().withName("full").withNamespace("name")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.INCREMENTAL).withStream(new AirbyteStream().withName("incre").withNamespace("name"))));

      final var streams = handler.getFullRefreshStreams(mCatalog, 1);
      final var exp = Set.of(new StreamDescriptor().withName("full"), new StreamDescriptor().withName("full").withNamespace("name"));
      assertEquals(exp, streams);
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2, 3}) // six numbers
    void createAttemptShouldAlwaysDeleteFullRefreshStreamState(int attemptNumber) throws IOException {
      final var connId = UUID.randomUUID();
      final Job mJob = mock(Job.class);
      when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.SYNC);
      when(mJob.getAttemptsCount()).thenReturn(0);
      when(mJob.getScope()).thenReturn(connId.toString());

      final var mConfig = mock(JobConfig.class);
      when(mJob.getConfig()).thenReturn(mConfig);

      final var mDyncConfig = mock(JobSyncConfig.class);
      when(mConfig.getSync()).thenReturn(mDyncConfig);
      when(mDyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

      when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);

      when(path.resolve(Mockito.anyString())).thenReturn(path);
      when(ffClient.boolVariation(any(), any())).thenReturn(true);

      final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
      final Path expectedLogPath = expectedRoot.resolve(LogClientSingleton.LOG_FILENAME);

      final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
      when(mDyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);

      when(mCatalog.getStreams()).thenReturn(List.of(
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.FULL_REFRESH).withStream(new AirbyteStream().withName("full")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.INCREMENTAL).withStream(new AirbyteStream().withName("incre")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.FULL_REFRESH).withStream(new AirbyteStream().withName("full").withNamespace("name")),
          new ConfiguredAirbyteStream().withSyncMode(SyncMode.INCREMENTAL).withStream(new AirbyteStream().withName("incre").withNamespace("name"))));

      when(jobPersistence.createAttempt(JOB_ID, expectedLogPath)).thenReturn(attemptNumber);

      final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
      assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);

      ArgumentCaptor<UUID> captor1 = ArgumentCaptor.forClass(UUID.class);
      ArgumentCaptor<Set<StreamDescriptor>> captor2 = ArgumentCaptor.forClass(Set.class);
      verify(statePersistence).bulkDelete(captor1.capture(), captor2.capture());
      assertEquals(connId, captor1.getValue());
      assertEquals(Set.of(new StreamDescriptor().withName("full"), new StreamDescriptor().withName("full").withNamespace("name")),
          captor2.getValue());
    }

  }

  @Test
  void createAttemptNumberWithUnknownJobId() throws IOException {
    final Job mJob = mock(Job.class);
    when(mJob.getAttemptsCount())
        .thenReturn(ATTEMPT_NUMBER);

    when(jobPersistence.getJob(JOB_ID))
        .thenThrow(new RuntimeException("unknown jobId " + JOB_ID));

    Assertions.assertThatThrownBy(() -> handler.createNewAttemptNumber(JOB_ID))
        .isInstanceOf(UnprocessableContentException.class);
  }

  @Test
  void getAttemptThrowsNotFound() throws Exception {
    when(jobPersistence.getAttemptForJob(anyLong(), anyInt())).thenReturn(Optional.empty());

    assertThrows(IdNotFoundKnownException.class, () -> handler.getAttemptForJob(1L, 2));
  }

  @Test
  void getAttemptReturnsAttempt() throws Exception {
    final var attempt = new Attempt(
        2,
        214L,
        Path.of("/tmp/logs/all/the/way/down"),
        null,
        null,
        AttemptStatus.SUCCEEDED,
        null,
        null,
        Instant.now().getEpochSecond(),
        Instant.now().getEpochSecond(),
        Instant.now().getEpochSecond());

    final var logs = new LogRead();
    logs.addLogLinesItem("log line 1");
    logs.addLogLinesItem("log line 2");
    final var infoRead = new AttemptInfoRead();
    infoRead.setAttempt(JobConverter.getAttemptRead(attempt));
    infoRead.setLogs(logs);

    when(jobPersistence.getAttemptForJob(anyLong(), anyInt())).thenReturn(Optional.of(attempt));
    when(jobConverter.getAttemptInfoRead(attempt)).thenReturn(infoRead);

    final AttemptInfoRead result = handler.getAttemptForJob(1L, 2);
    assertEquals(attempt.getAttemptNumber(), result.getAttempt().getId());
    assertEquals(attempt.getEndedAtInSecond().get(), result.getAttempt().getEndedAt());
    assertEquals(attempt.getCreatedAtInSecond(), result.getAttempt().getCreatedAt());
    assertEquals(attempt.getUpdatedAtInSecond(), result.getAttempt().getUpdatedAt());
    assertEquals(io.airbyte.api.model.generated.AttemptStatus.SUCCEEDED, result.getAttempt().getStatus());
    assertEquals(logs, result.getLogs());
  }

  @Test
  void getAttemptCombinedStatsThrowsNotFound() throws Exception {
    when(jobPersistence.getAttemptCombinedStats(anyLong(), anyInt())).thenReturn(null);

    assertThrows(IdNotFoundKnownException.class, () -> handler.getAttemptCombinedStats(1L, 2));
  }

  @Test
  void getAttemptCombinedStatsReturnsStats() throws Exception {
    final var stats = new SyncStats();
    stats.setRecordsEmitted(123L);
    stats.setBytesEmitted(123L);
    stats.setBytesCommitted(123L);
    stats.setRecordsCommitted(123L);
    stats.setEstimatedRecords(123L);
    stats.setEstimatedBytes(123L);

    when(jobPersistence.getAttemptCombinedStats(anyLong(), anyInt())).thenReturn(stats);

    final AttemptStats result = handler.getAttemptCombinedStats(1L, 2);
    assertEquals(stats.getRecordsEmitted(), result.getRecordsEmitted());
    assertEquals(stats.getBytesEmitted(), result.getBytesEmitted());
    assertEquals(stats.getBytesCommitted(), result.getBytesCommitted());
    assertEquals(stats.getRecordsCommitted(), result.getRecordsCommitted());
    assertEquals(stats.getEstimatedRecords(), result.getEstimatedRecords());
    assertEquals(stats.getEstimatedBytes(), result.getEstimatedBytes());
    assertNull(result.getStateMessagesEmitted()); // punting on this for now
  }

  @Test
  void failAttemptSyncSummaryOutputPresent() throws IOException {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, standardSyncOutput);

    verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary);
  }

  @Test
  void failAttemptSyncSummaryOutputNotPresent() throws IOException {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, null);

    verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobPersistence, never()).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary);
  }

  @Test
  void failAttemptSyncSummaryNotPresent() throws IOException {
    handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, null, standardSyncOutput);

    verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, null);
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  void failAttemptValidatesFailureSummary(final Object thing) {
    assertThrows(BadRequestException.class, () -> handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, thing, standardSyncOutput));
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  void failAttemptValidatesSyncOutput(final Object thing) {
    assertThrows(BadRequestException.class, () -> handler.failAttempt(ATTEMPT_NUMBER, JOB_ID, failureSummary, thing));
  }

  private static Stream<Arguments> randomObjects() {
    return Stream.of(
        Arguments.of(123L),
        Arguments.of(true),
        Arguments.of(List.of("123", "123")),
        Arguments.of("a string"),
        Arguments.of(543.0));
  }

}
