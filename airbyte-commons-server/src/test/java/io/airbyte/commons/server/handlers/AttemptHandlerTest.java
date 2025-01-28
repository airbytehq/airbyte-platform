/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.logging.LogMdcHelperKt.DEFAULT_LOG_FILENAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptInfoReadLogs;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.AttemptSyncConfig;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.model.generated.GlobalState;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SaveStreamAttemptMetadataRequestBody;
import io.airbyte.api.model.generated.StreamAttemptMetadata;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnprocessableContentException;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.config.persistence.helper.GenerationBumper;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.StreamAttemptMetadataService;
import io.airbyte.featureflag.EnableResumableFullRefresh;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class AttemptHandlerTest {

  private final JobConverter jobConverter = mock(JobConverter.class);
  private final JobPersistence jobPersistence = mock(JobPersistence.class);
  private final StatePersistence statePersistence = mock(StatePersistence.class);
  private final Path path = mock(Path.class);
  private final JobCreationAndStatusUpdateHelper helper = mock(JobCreationAndStatusUpdateHelper.class);
  private final FeatureFlagClient ffClient = mock(TestClient.class);
  private final GenerationBumper generationBumper = mock(GenerationBumper.class);
  private final ConnectionService connectionService = mock(ConnectionService.class);
  private final DestinationService destinationService = mock(DestinationService.class);
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
  private final StreamAttemptMetadataService streamAttemptMetadataService = mock(StreamAttemptMetadataService.class);

  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  private final AttemptHandler handler = new AttemptHandler(jobPersistence,
      statePersistence,
      jobConverter,
      ffClient,
      helper,
      path,
      generationBumper,
      connectionService,
      destinationService,
      actorDefinitionVersionHelper,
      streamAttemptMetadataService,
      apiPojoConverters);

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final long JOB_ID = 10002L;
  private static final int ATTEMPT_NUMBER = 1;
  private static final String PROCESSING_TASK_QUEUE = "SYNC";

  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED));

  private static final JobOutput jobOutput = new JobOutput().withSync(standardSyncOutput);

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

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

    final io.airbyte.config.AttemptSyncConfig expectedAttemptSyncConfig = apiPojoConverters.attemptSyncConfigToInternal(attemptSyncConfig);

    assertEquals(ATTEMPT_NUMBER, attemptNumberCapture.getValue());
    assertEquals(JOB_ID, jobIdCapture.getValue());
    assertEquals(expectedAttemptSyncConfig, attemptSyncConfigCapture.getValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void createAttemptNumberForSync(final boolean enableRfr)
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final int attemptNumber = 0;
    final var connId = UUID.randomUUID();
    final Job mJob = mock(Job.class);
    when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.SYNC);
    when(mJob.getAttemptsCount()).thenReturn(ATTEMPT_NUMBER);
    when(mJob.getScope()).thenReturn(connId.toString());
    when(mJob.getId()).thenReturn(JOB_ID);

    final var mConfig = mock(JobConfig.class);
    when(mConfig.getConfigType()).thenReturn(ConfigType.SYNC);
    when(mJob.getConfig()).thenReturn(mConfig);

    final var mSyncConfig = mock(JobSyncConfig.class);
    when(mConfig.getSync()).thenReturn(mSyncConfig);
    when(mSyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

    final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
    when(mSyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);
    when(mCatalog.getStreams()).thenReturn(List.of(
        new ConfiguredAirbyteStream(
            new AirbyteStream("rfrStream", Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH)).withIsResumable(true),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

    when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);
    when(path.resolve(Mockito.anyString())).thenReturn(path);

    final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
    final Path expectedLogPath = expectedRoot.resolve(DEFAULT_LOG_FILENAME);

    when(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
        .thenReturn(attemptNumber);
    when(ffClient.boolVariation(any(), any())).thenReturn(true);
    when(ffClient.boolVariation(eq(EnableResumableFullRefresh.INSTANCE), any())).thenReturn(enableRfr);
    StateWrapper stateWrapper = new StateWrapper().withStateType(StateType.STREAM);
    if (enableRfr) {
      when(statePersistence.getCurrentState(connId)).thenReturn(Optional.of(stateWrapper));
    }
    final UUID destinationId = UUID.randomUUID();
    when(connectionService.getStandardSync(connId)).thenReturn(new StandardSync().withDestinationId(destinationId));
    when(destinationService.getDestinationConnection(destinationId))
        .thenReturn(new DestinationConnection().withWorkspaceId(WORKSPACE_ID));
    when(actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any()))
        .thenReturn(new ActorDefinitionVersion().withSupportsRefreshes(enableRfr));
    final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
    assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);
    if (enableRfr) {
      verify(generationBumper).updateGenerationForStreams(connId, JOB_ID, List.of(), Set.of(new StreamDescriptor().withName("rfrStream")));
      verify(statePersistence).bulkDelete(connId, Set.of(new StreamDescriptor().withName("rfrStream")));
    }
  }

  @Test
  void createAttemptNumberForClear()
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final int attemptNumber = 0;
    final var connId = UUID.randomUUID();
    final Job mJob = mock(Job.class);
    when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.CLEAR);
    when(mJob.getAttemptsCount()).thenReturn(ATTEMPT_NUMBER);
    when(mJob.getScope()).thenReturn(connId.toString());
    when(mJob.getId()).thenReturn(JOB_ID);

    final var mConfig = mock(JobConfig.class);
    when(mConfig.getConfigType()).thenReturn(ConfigType.CLEAR);
    when(mJob.getConfig()).thenReturn(mConfig);

    final var mResetConfig = mock(JobResetConnectionConfig.class);
    when(mConfig.getResetConnection()).thenReturn(mResetConfig);
    when(mResetConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

    final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
    when(mResetConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);
    when(mCatalog.getStreams()).thenReturn(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream("rfrStream", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)).withIsResumable(true),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND)));
    when(mResetConfig.getResetSourceConfiguration()).thenReturn(new ResetSourceConfiguration().withStreamsToReset(
        List.of(new StreamDescriptor().withName("rfrStream"))));

    when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);
    when(path.resolve(Mockito.anyString())).thenReturn(path);

    final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
    final Path expectedLogPath = expectedRoot.resolve(DEFAULT_LOG_FILENAME);

    when(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
        .thenReturn(attemptNumber);
    when(ffClient.boolVariation(any(), any())).thenReturn(true);
    final UUID destinationId = UUID.randomUUID();
    when(connectionService.getStandardSync(connId)).thenReturn(new StandardSync().withDestinationId(destinationId));
    when(destinationService.getDestinationConnection(destinationId))
        .thenReturn(new DestinationConnection().withWorkspaceId(WORKSPACE_ID));
    when(actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any()))
        .thenReturn(new ActorDefinitionVersion().withSupportsRefreshes(true));
    final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
    assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);
    verify(generationBumper).updateGenerationForStreams(connId, JOB_ID, List.of(), Set.of(new StreamDescriptor().withName("rfrStream")));
    verify(statePersistence).bulkDelete(connId, Set.of(new StreamDescriptor().withName("rfrStream")));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void createAttemptNumberForRefresh(final int attemptNumber)
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final var connId = UUID.randomUUID();
    final Job mJob = mock(Job.class);
    when(mJob.getConfigType()).thenReturn(ConfigType.REFRESH);
    when(mJob.getAttemptsCount()).thenReturn(ATTEMPT_NUMBER);
    when(mJob.getScope()).thenReturn(connId.toString());
    when(mJob.getId()).thenReturn(JOB_ID);

    final var mConfig = mock(JobConfig.class);
    when(mConfig.getConfigType()).thenReturn(ConfigType.REFRESH);
    when(mJob.getConfig()).thenReturn(mConfig);

    final var mRefreshConfig = mock(RefreshConfig.class);
    when(mConfig.getRefresh()).thenReturn(mRefreshConfig);
    when(mRefreshConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

    final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
    when(mRefreshConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);
    when(mCatalog.getStreams()).thenReturn(List.of(
        new ConfiguredAirbyteStream(
            new AirbyteStream("rfrStream", Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH)).withIsResumable(true),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND),
        new ConfiguredAirbyteStream(new AirbyteStream("nonRfrStream", Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND)));

    when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);
    when(path.resolve(Mockito.anyString())).thenReturn(path);

    final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
    final Path expectedLogPath = expectedRoot.resolve(DEFAULT_LOG_FILENAME);

    when(jobPersistence.createAttempt(JOB_ID, expectedLogPath))
        .thenReturn(attemptNumber);
    when(ffClient.boolVariation(any(), any())).thenReturn(true);
    when(ffClient.boolVariation(eq(EnableResumableFullRefresh.INSTANCE), any())).thenReturn(true);
    final UUID destinationId = UUID.randomUUID();
    when(connectionService.getStandardSync(connId)).thenReturn(new StandardSync().withDestinationId(destinationId));
    when(destinationService.getDestinationConnection(destinationId))
        .thenReturn(new DestinationConnection().withWorkspaceId(WORKSPACE_ID));
    when(actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any()))
        .thenReturn(new ActorDefinitionVersion().withSupportsRefreshes(true));
    final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
    assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);
    if (attemptNumber == 0) {
      verify(generationBumper).updateGenerationForStreams(connId, JOB_ID, List.of(), Set.of(
          new StreamDescriptor().withName("rfrStream"),
          new StreamDescriptor().withName("nonRfrStream")));
      verify(statePersistence).bulkDelete(connId, Set.of(
          new StreamDescriptor().withName("rfrStream"),
          new StreamDescriptor().withName("nonRfrStream")));
    } else {
      verify(generationBumper).updateGenerationForStreams(connId, JOB_ID, List.of(), Set.of(new StreamDescriptor().withName("nonRfrStream")));
      verify(statePersistence).bulkDelete(connId, Set.of(new StreamDescriptor().withName("nonRfrStream")));
    }
  }

  @Nested
  class ClearFullRefreshStreamStateFirstAttempt {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void getFullRefreshStreamsShouldOnlyReturnFullRefreshStreams(final boolean enableResumableFullRefresh) {
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
          new ConfiguredAirbyteStream(
              new AirbyteStream("full", Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH)).withIsResumable(true),
              SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(new AirbyteStream("incre", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(
              new AirbyteStream("full", Jsons.emptyObject(), List.of(io.airbyte.config.SyncMode.FULL_REFRESH)).withNamespace("name"),
              SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(new AirbyteStream("incre", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)).withNamespace("name"),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND)));

      final var streams = handler.getFullRefreshStreamsToClear(mCatalog, 1, enableResumableFullRefresh);
      final var exp = enableResumableFullRefresh ? Set.of(new StreamDescriptor().withName("full").withNamespace("name"))
          : Set.of(new StreamDescriptor().withName("full"), new StreamDescriptor().withName("full").withNamespace("name"));
      assertEquals(exp, streams);
    }

    private static Stream<Arguments> provideCreateAttemptConfig() {
      return Stream.of(
          Arguments.of(0, true),
          Arguments.of(1, true),
          Arguments.of(2, true),
          Arguments.of(3, true),
          Arguments.of(0, false),
          Arguments.of(1, false),
          Arguments.of(2, false),
          Arguments.of(3, false));
    }

    @ParameterizedTest()
    @MethodSource("provideCreateAttemptConfig")
    void createAttemptShouldAlwaysDeleteFullRefreshStreamState(final int attemptNumber, final boolean enableResumableFullRefresh)
        throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
      final var connId = UUID.randomUUID();
      final Job mJob = mock(Job.class);
      when(mJob.getConfigType()).thenReturn(JobConfig.ConfigType.SYNC);
      when(mJob.getAttemptsCount()).thenReturn(0);
      when(mJob.getScope()).thenReturn(connId.toString());
      when(mJob.getId()).thenReturn(JOB_ID);

      final var mConfig = mock(JobConfig.class);
      when(mConfig.getConfigType()).thenReturn(ConfigType.SYNC);
      when(mJob.getConfig()).thenReturn(mConfig);

      final var mDyncConfig = mock(JobSyncConfig.class);
      when(mConfig.getSync()).thenReturn(mDyncConfig);
      when(mDyncConfig.getWorkspaceId()).thenReturn(UUID.randomUUID());

      when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);

      when(path.resolve(Mockito.anyString())).thenReturn(path);
      when(ffClient.boolVariation(any(), any())).thenReturn(true);
      when(ffClient.boolVariation(eq(EnableResumableFullRefresh.INSTANCE), any())).thenReturn(enableResumableFullRefresh);
      final Path expectedRoot = TemporalUtils.getJobRoot(path, String.valueOf(JOB_ID), ATTEMPT_NUMBER);
      final Path expectedLogPath = expectedRoot.resolve(DEFAULT_LOG_FILENAME);

      final var mCatalog = mock(ConfiguredAirbyteCatalog.class);
      when(mDyncConfig.getConfiguredAirbyteCatalog()).thenReturn(mCatalog);

      when(mCatalog.getStreams()).thenReturn(List.of(
          new ConfiguredAirbyteStream(new AirbyteStream("full", Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)).withIsResumable(true),
              SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(new AirbyteStream("incre", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(new AirbyteStream("full", Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)).withNamespace("name"),
              SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(new AirbyteStream("incre", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)).withNamespace("name"),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND)));

      when(jobPersistence.createAttempt(JOB_ID, expectedLogPath)).thenReturn(attemptNumber);

      final UUID destinationId = UUID.randomUUID();
      when(connectionService.getStandardSync(connId)).thenReturn(new StandardSync().withDestinationId(destinationId));
      when(destinationService.getDestinationConnection(destinationId))
          .thenReturn(new DestinationConnection().withWorkspaceId(WORKSPACE_ID));
      when(actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any()))
          .thenReturn(new ActorDefinitionVersion().withSupportsRefreshes(true));

      final CreateNewAttemptNumberResponse output = handler.createNewAttemptNumber(JOB_ID);
      assertThat(output.getAttemptNumber()).isEqualTo(attemptNumber);

      ArgumentCaptor<UUID> captor1 = ArgumentCaptor.forClass(UUID.class);
      ArgumentCaptor<Set<StreamDescriptor>> captor2 = ArgumentCaptor.forClass(Set.class);
      verify(statePersistence).bulkDelete(captor1.capture(), captor2.capture());
      assertEquals(connId, captor1.getValue());
      if (enableResumableFullRefresh) {
        Set<StreamDescriptor> nonResumableFullRefresh = attemptNumber == 0 ? Set.of(
            new StreamDescriptor().withName("full"),
            new StreamDescriptor().withName("full").withNamespace("name")) : Set.of(new StreamDescriptor().withName("full").withNamespace("name"));
        assertEquals(nonResumableFullRefresh, captor2.getValue());
        verify(generationBumper).updateGenerationForStreams(connId, JOB_ID, List.of(), nonResumableFullRefresh);
      } else {
        assertEquals(Set.of(new StreamDescriptor().withName("full"), new StreamDescriptor().withName("full").withNamespace("name")),
            captor2.getValue());
      }
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

    final var logs = new AttemptInfoReadLogs();
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

  @Test
  void saveStreamMetadata() {
    final long jobId = 123L;
    final int attemptNumber = 1;

    final var result = handler.saveStreamMetadata(new SaveStreamAttemptMetadataRequestBody()
        .jobId(jobId)
        .attemptNumber(attemptNumber)
        .streamMetadata(List.of(
            new StreamAttemptMetadata().streamName("s1").wasBackfilled(false).wasResumed(true),
            new StreamAttemptMetadata().streamName("s2").streamNamespace("ns").wasBackfilled(true).wasResumed(false))));
    verify(streamAttemptMetadataService).upsertStreamAttemptMetadata(
        jobId,
        attemptNumber,
        List.of(
            new io.airbyte.data.services.StreamAttemptMetadata("s1", null, false, true),
            new io.airbyte.data.services.StreamAttemptMetadata("s2", "ns", true, false)));
    assertEquals(new InternalOperationResult().succeeded(true), result);
  }

  @Test
  void saveStreamMetadataFailure() {
    final long jobId = 123L;
    final int attemptNumber = 1;

    doThrow(new RuntimeException("oops")).when(streamAttemptMetadataService).upsertStreamAttemptMetadata(anyLong(), anyLong(), any());

    final var result = handler.saveStreamMetadata(new SaveStreamAttemptMetadataRequestBody()
        .jobId(jobId)
        .attemptNumber(attemptNumber)
        .streamMetadata(List.of(new StreamAttemptMetadata().streamName("s").wasBackfilled(false).wasResumed(false))));
    assertEquals(new InternalOperationResult().succeeded(false), result);
  }

  private static final String STREAM_INCREMENTAL = "incremental";
  private static final String STREAM_INCREMENTAL_NOT_RESUMABLE = "incremental not resumable";
  private static final String STREAM_FULL_REFRESH_RESUMABLE = "full refresh resumable";
  private static final String STREAM_FULL_REFRESH_NOT_RESUMABLE = "full refresh not resumable";

  private static Stream<Arguments> testStateClearingLogic() {
    return Stream.of(
        // streams are STREAM_INCREMENTAL, STREAM_INCREMENTAL_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE,
        // STREAM_FULL_REFRESH_NOT_RESUMABLE
        // AttemptNumber, SupportsRefresh, streams to clear
        Arguments.of(0, false, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(0, true, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(1, false, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE, STREAM_FULL_REFRESH_RESUMABLE)),
        Arguments.of(1, true, streamDescriptorsFromNames(STREAM_FULL_REFRESH_NOT_RESUMABLE)));
  }

  @ParameterizedTest
  @MethodSource
  void testStateClearingLogic(final int attemptNumber, final boolean supportsRefresh, final Set<StreamDescriptor> expectedStreamsToClear)
      throws Exception {
    when(ffClient.boolVariation(eq(EnableResumableFullRefresh.INSTANCE), any())).thenReturn(true);
    var configuredCatalog = new ConfiguredAirbyteCatalog(
        List.of(
            buildStreamForClearStateTest(STREAM_INCREMENTAL_NOT_RESUMABLE, SyncMode.INCREMENTAL, false),
            buildStreamForClearStateTest(STREAM_INCREMENTAL, SyncMode.INCREMENTAL, true),
            buildStreamForClearStateTest(STREAM_FULL_REFRESH_NOT_RESUMABLE, SyncMode.FULL_REFRESH, false),
            buildStreamForClearStateTest(STREAM_FULL_REFRESH_RESUMABLE, SyncMode.FULL_REFRESH, true)));

    final long jobId = 123L;
    final UUID connectionId = UUID.randomUUID();
    var job = mock(Job.class);
    when(job.getId()).thenReturn(jobId);
    when(job.getScope()).thenReturn(connectionId.toString());
    when(job.getConfigType()).thenReturn(ConfigType.SYNC);
    when(job.getConfig())
        .thenReturn(new JobConfig().withConfigType(ConfigType.SYNC).withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(configuredCatalog)));

    if (attemptNumber == 0) {
      handler.updateGenerationAndStateForFirstAttempt(job, connectionId, supportsRefresh);
    } else {
      handler.updateGenerationAndStateForSubsequentAttempts(job, supportsRefresh);
    }
    verify(statePersistence).bulkDelete(connectionId, expectedStreamsToClear);
  }

  private static ConfiguredAirbyteStream buildStreamForClearStateTest(final String streamName, final SyncMode syncMode, final boolean isResumable) {
    return new ConfiguredAirbyteStream(new AirbyteStream(streamName, Jsons.emptyObject(), List.of(syncMode)).withIsResumable(isResumable))
        .withSyncMode(syncMode);
  }

  private static Set<StreamDescriptor> streamDescriptorsFromNames(final String... streamNames) {
    return Arrays.stream(streamNames).map(n -> new StreamDescriptor().withName(n)).collect(Collectors.toSet());
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
