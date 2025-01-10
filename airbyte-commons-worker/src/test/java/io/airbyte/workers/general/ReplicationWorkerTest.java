/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.commons.logging.LogMdcHelperKt.DEFAULT_JOB_LOG_PATH_MDC_KEY;
import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT;
import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT;
import static io.airbyte.workers.test_utils.TestConfigHelpers.DESTINATION_IMAGE;
import static io.airbyte.workers.test_utils.TestConfigHelpers.SOURCE_IMAGE;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ActorDefinitionVersionApi;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.string.Strings;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.mappers.application.RecordMapper;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteLogMessage.Level;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.Config;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.DestinationTimeoutMonitor.TimeoutException;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.SimpleAirbyteDestination;
import io.airbyte.workers.internal.SimpleAirbyteSource;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import io.airbyte.workers.test_utils.TestConfigHelpers;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Implementation agnostic tests for the Replication worker.
 */
abstract class ReplicationWorkerTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ReplicationWorkerTest.class);

  protected static final String JOB_ID = "0";
  protected static final int JOB_ATTEMPT = 0;
  protected static final Path WORKSPACE_ROOT = Path.of("workspaces/10");
  protected static final String STREAM_NAME = "user_preferences";
  protected static final String FIELD_NAME = "favorite_color";
  protected static final AirbyteMessage RECORD_MESSAGE1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "blue");
  protected static final AirbyteMessage RECORD_MESSAGE2 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "yellow");
  protected static final AirbyteMessage RECORD_MESSAGE3 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, 3);
  protected static final AirbyteMessage STATE_MESSAGE = AirbyteMessageUtils.createStateMessage(STREAM_NAME, "checkpoint", "1");
  protected static final AirbyteTraceMessage ERROR_TRACE_MESSAGE =
      AirbyteMessageUtils.createErrorTraceMessage("a connector error occurred", Double.valueOf(123));

  protected static final Config CONNECTOR_CONFIG = new Config().withAdditionalProperty("my_key", "my_new_value");
  protected static final AirbyteMessage CONFIG_MESSAGE = AirbyteMessageUtils.createConfigControlMessage(CONNECTOR_CONFIG, 1D);
  protected static final String STREAM1 = "stream1";

  protected static final String NAMESPACE = "namespace";
  protected static final String INDUCED_EXCEPTION = "induced exception";
  protected static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  protected static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();

  protected Path jobRoot;
  protected SimpleAirbyteSource sourceStub;
  protected AirbyteSource source;
  protected NamespacingMapper mapper;
  protected AirbyteDestination destination;
  protected ReplicationInput replicationInput;
  protected WorkerSourceConfig sourceConfig;
  protected WorkerDestinationConfig destinationConfig;
  protected AirbyteMessageTracker messageTracker;
  protected SyncStatsTracker syncStatsTracker;
  protected SyncPersistence syncPersistence;
  protected RecordSchemaValidator recordSchemaValidator;
  protected MetricClient metricClient;
  protected WorkerMetricReporter workerMetricReporter;
  protected ConnectorConfigUpdater connectorConfigUpdater;
  protected HeartbeatTimeoutChaperone heartbeatTimeoutChaperone;
  protected ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  protected AirbyteMessageDataExtractor airbyteMessageDataExtractor;
  protected ReplicationFeatureFlagReader replicationFeatureFlagReader;
  protected DestinationTimeoutMonitor destinationTimeoutMonitor;

  protected VoidCallable onReplicationRunning;

  protected ReplicationWorkerHelper replicationWorkerHelper;
  protected WorkloadApi workloadApi;
  protected WorkloadApiClient workloadApiClient;
  protected AirbyteApiClient airbyteApiClient;
  protected AnalyticsMessageTracker analyticsMessageTracker;
  protected SourceApi sourceApi;
  protected DestinationApi destinationApi;
  protected StreamStatusCompletionTracker streamStatusCompletionTracker;
  protected ActorDefinitionVersionApi actorDefinitionVersionApi;
  protected StreamStatusTrackerFactory streamStatusTrackerFactory;
  protected RecordMapper recordMapper;
  protected FeatureFlagClient featureFlagClient;
  protected DestinationCatalogGenerator destinationCatalogGenerator;

  BufferedReplicationWorker getDefaultReplicationWorker() {
    return getDefaultReplicationWorker(false);
  }

  // Entrypoint to override for ReplicationWorker implementations
  abstract BufferedReplicationWorker getDefaultReplicationWorker(final boolean fieldSelectionEnabled);

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() throws Exception {
    MDC.clear();

    jobRoot = Files.createDirectories(Files.createTempDirectory("test").resolve(WORKSPACE_ROOT));

    final ImmutablePair<StandardSync, ReplicationInput> syncPair = TestConfigHelpers.createReplicationConfig();
    replicationInput = syncPair.getValue();

    sourceConfig = WorkerUtils.syncToWorkerSourceConfig(replicationInput);
    destinationConfig = WorkerUtils.syncToWorkerDestinationConfig(replicationInput);

    sourceStub = new SimpleAirbyteSource();
    sourceStub.setMessages(RECORD_MESSAGE1, RECORD_MESSAGE2);
    source = spy(sourceStub);

    mapper = mock(NamespacingMapper.class);
    destination = spy(new SimpleAirbyteDestination());
    messageTracker = mock(AirbyteMessageTracker.class);
    syncStatsTracker = mock(SyncStatsTracker.class);
    syncPersistence = mock(SyncPersistence.class);
    recordSchemaValidator = mock(RecordSchemaValidator.class);
    connectorConfigUpdater = mock(ConnectorConfigUpdater.class);
    metricClient = mock(MetricClient.class);
    workerMetricReporter = new WorkerMetricReporter(metricClient, "docker_image:v1.0.0");
    airbyteMessageDataExtractor = new AirbyteMessageDataExtractor();
    onReplicationRunning = mock(VoidCallable.class);
    replicationFeatureFlagReader = mock(ReplicationFeatureFlagReader.class);

    final HeartbeatMonitor heartbeatMonitor = mock(HeartbeatMonitor.class);
    heartbeatTimeoutChaperone =
        new HeartbeatTimeoutChaperone(heartbeatMonitor, Duration.ofMinutes(5), null, null, null, "docker image", metricClient);
    destinationTimeoutMonitor = mock(DestinationTimeoutMonitor.class);
    replicationAirbyteMessageEventPublishingHelper = mock(ReplicationAirbyteMessageEventPublishingHelper.class);
    workloadApi = mock(WorkloadApi.class);
    workloadApiClient = mock(WorkloadApiClient.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    when(workloadApiClient.getWorkloadApi()).thenReturn(workloadApi);

    analyticsMessageTracker = mock(AnalyticsMessageTracker.class);

    sourceApi = mock(SourceApi.class);
    when(sourceApi.getSource(any())).thenReturn(new SourceRead(
        SOURCE_DEFINITION_ID,
        UUID.randomUUID(),
        UUID.randomUUID(),
        Jsons.jsonNode(Map.of()),
        "name",
        "source-name",
        1L,
        null,
        null,
        null,
        null, null));
    destinationApi = mock(DestinationApi.class);
    when(destinationApi.getDestination(any())).thenReturn(new DestinationRead(
        DESTINATION_DEFINITION_ID,
        UUID.randomUUID(),
        UUID.randomUUID(),
        Jsons.jsonNode(Map.of()),
        "name",
        "destination-name",
        1L,
        null,
        null,
        null,
        null, null));
    streamStatusCompletionTracker = mock(StreamStatusCompletionTracker.class);
    streamStatusTrackerFactory = mock(StreamStatusTrackerFactory.class);
    when(streamStatusTrackerFactory.create(any())).thenReturn(mock(StreamStatusTracker.class));

    when(airbyteApiClient.getDestinationApi()).thenReturn(destinationApi);
    when(airbyteApiClient.getSourceApi()).thenReturn(sourceApi);

    actorDefinitionVersionApi = mock(ActorDefinitionVersionApi.class);
    var resolveActorDefinitionVersionResponse = new ResolveActorDefinitionVersionResponse(UUID.randomUUID(),
        "dockerRepository",
        "dockerImageTag",
        false,
        false);
    when(actorDefinitionVersionApi.resolveActorDefinitionVersionByTag(any())).thenReturn(resolveActorDefinitionVersionResponse);
    when(airbyteApiClient.getActorDefinitionVersionApi()).thenReturn(actorDefinitionVersionApi);

    when(messageTracker.getSyncStatsTracker()).thenReturn(syncStatsTracker);

    when(mapper.mapCatalog(destinationConfig.getCatalog())).thenReturn(destinationConfig.getCatalog());
    when(mapper.mapMessage(RECORD_MESSAGE1)).thenReturn(RECORD_MESSAGE1);
    when(mapper.mapMessage(RECORD_MESSAGE2)).thenReturn(RECORD_MESSAGE2);
    when(mapper.mapMessage(RECORD_MESSAGE3)).thenReturn(RECORD_MESSAGE3);
    when(mapper.mapMessage(CONFIG_MESSAGE)).thenReturn(CONFIG_MESSAGE);
    when(mapper.revertMap(STATE_MESSAGE)).thenReturn(STATE_MESSAGE);
    when(mapper.revertMap(CONFIG_MESSAGE)).thenReturn(CONFIG_MESSAGE);
    when(replicationFeatureFlagReader.readReplicationFeatureFlags()).thenReturn(
        new ReplicationFeatureFlags(false, 60, 4, false, false, false));
    when(heartbeatMonitor.isBeating()).thenReturn(Optional.of(true));

    recordMapper = mock(RecordMapper.class);
    featureFlagClient = mock(TestClient.class);
    destinationCatalogGenerator = mock(DestinationCatalogGenerator.class);
    when(destinationCatalogGenerator.generateDestinationCatalog(any()))
        .thenReturn(new DestinationCatalogGenerator.CatalogGenerationResult(destinationConfig.getCatalog(), Map.of()));

    MDC.put(DEFAULT_JOB_LOG_PATH_MDC_KEY, jobRoot.toString());
    LogSource.PLATFORM.toMdc().forEach(MDC::put);
  }

  @AfterEach
  void tearDown() {
    Mockito.framework().clearInlineMocks();
    MDC.clear();
  }

  @Test
  void test() throws Exception {
    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(source).start(sourceConfig, jobRoot, replicationInput.getConnectionId());
    verify(destination).start(destinationConfig, jobRoot);
    verify(onReplicationRunning).call();
    verify(replicationWorkerHelper).applyTransformationMappers(new AirbyteJsonRecordAdapter(RECORD_MESSAGE1));
    verify(replicationWorkerHelper).applyTransformationMappers(new AirbyteJsonRecordAdapter(RECORD_MESSAGE2));
    verify(destination).accept(RECORD_MESSAGE1);
    verify(destination).accept(RECORD_MESSAGE2);
    verify(source, atLeastOnce()).close();
    verify(destination).close();
    verify(recordSchemaValidator).validateSchema(
        RECORD_MESSAGE1.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(RECORD_MESSAGE1.getRecord()),
        new ConcurrentHashMap<>());
    verify(recordSchemaValidator).validateSchema(
        RECORD_MESSAGE2.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(RECORD_MESSAGE2.getRecord()),
        new ConcurrentHashMap<>());
  }

  @Test
  void testReplicationTimesAreUpdated() throws Exception {
    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);

    final SyncStats syncStats = output.getReplicationAttemptSummary().getTotalStats();
    assertNotEquals(0, syncStats.getReplicationStartTime());
    assertNotEquals(0, syncStats.getReplicationEndTime());
    assertNotEquals(0, syncStats.getSourceReadStartTime());
    assertNotEquals(0, syncStats.getSourceReadEndTime());
    assertNotEquals(0, syncStats.getDestinationWriteStartTime());
    assertNotEquals(0, syncStats.getDestinationWriteEndTime());
  }

  @Test
  void testInvalidSchema() throws Exception {
    sourceStub.setMessages(RECORD_MESSAGE1, RECORD_MESSAGE2, RECORD_MESSAGE3);

    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(source).start(sourceConfig, jobRoot, replicationInput.getConnectionId());
    verify(destination).start(destinationConfig, jobRoot);
    verify(destination).accept(RECORD_MESSAGE1);
    verify(destination).accept(RECORD_MESSAGE2);
    verify(destination).accept(RECORD_MESSAGE3);
    verify(recordSchemaValidator).validateSchema(
        RECORD_MESSAGE1.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(RECORD_MESSAGE1.getRecord()),
        new ConcurrentHashMap<>());
    verify(recordSchemaValidator).validateSchema(
        RECORD_MESSAGE2.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(RECORD_MESSAGE2.getRecord()),
        new ConcurrentHashMap<>());
    verify(recordSchemaValidator).validateSchema(
        RECORD_MESSAGE3.getRecord(),
        AirbyteStreamNameNamespacePair.fromRecordMessage(RECORD_MESSAGE3.getRecord()),
        new ConcurrentHashMap<>());
    verify(source).close();
    verify(destination).close();
  }

  @Test
  void testWorkerShutsDownLongRunningSchemaValidationThread() throws Exception {
    final String streamName = sourceConfig.getCatalog().getStreams().get(0).getStream().getName();
    final String streamNamespace = sourceConfig.getCatalog().getStreams().get(0).getStream().getNamespace();
    final ExecutorService executorService = Executors.newFixedThreadPool(1);
    final JsonSchemaValidator jsonSchemaValidator = mock(JsonSchemaValidator.class);
    recordSchemaValidator = new RecordSchemaValidator(Map.of(new AirbyteStreamNameNamespacePair(streamName, streamNamespace),
        sourceConfig.getCatalog().getStreams().get(0).getStream().getJsonSchema()), executorService, jsonSchemaValidator);

    final CountDownLatch countDownLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      // Make the schema validation thread artificially hang so that we can test the behavior
      // of what happens in the case the schema validation thread takes longer than the worker
      countDownLatch.await(1, TimeUnit.MINUTES);
      return null;
    }).when(jsonSchemaValidator).validateInitializedSchema(any(), any());

    final var worker = getDefaultReplicationWorker();
    worker.run(replicationInput, jobRoot);

    verify(source).start(sourceConfig, jobRoot, replicationInput.getConnectionId());
    verify(destination).start(destinationConfig, jobRoot);
    verify(destination).accept(RECORD_MESSAGE1);
    verify(destination).accept(RECORD_MESSAGE2);
    verify(source, atLeastOnce()).close();
    verify(destination).close();

    // We want to ensure the thread is forcibly shut down by the worker (not running even though
    // it should run for at least 2 minutes, in this test's mock) so that we never write to
    // validationErrors while the metricReporter is trying to read from it.
    assertTrue(executorService.isShutdown());

    // Since the thread was left to hang after the first call, we expect 1, not 2, calls to
    // validateInitializedSchema by the time the replication worker is done and shuts down the
    // validation thread. We therefore expect the metricReporter to only report on the first record.
    // NOTE: we use atMost because validateInitializedSchema is called async, and isn't guaranteed to be
    // invoked at all. The test still verifies that we shut it down without validating the second
    // record.
    verify(jsonSchemaValidator, Mockito.atMost(1)).validateInitializedSchema(any(), any());
  }

  @Test
  void testSourceNonZeroExitValue() throws Exception {
    when(source.getExitValue()).thenReturn(1);
    final var worker = getDefaultReplicationWorker();
    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream().anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.SOURCE)));
  }

  @Test
  void testReplicationRunnableSourceFailure() throws Exception {
    final String sourceErrorMessage = "the source had a failure";

    when(source.attemptRead()).thenThrow(new RuntimeException(sourceErrorMessage));

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.SOURCE) && f.getStacktrace().contains(sourceErrorMessage)));
  }

  @Test
  void testReplicationRunnableSourceUpdateConfig() throws Exception {
    sourceStub.setMessages(RECORD_MESSAGE1, CONFIG_MESSAGE);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.COMPLETED, output.getReplicationAttemptSummary().getStatus());

    verify(replicationAirbyteMessageEventPublishingHelper).publishEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE,
        CONFIG_MESSAGE,
        new ReplicationContext(false, replicationInput.getConnectionId(), replicationInput.getSourceId(), replicationInput.getDestinationId(),
            Long.parseLong(JOB_ID), JOB_ATTEMPT, replicationInput.getWorkspaceId(), SOURCE_IMAGE, DESTINATION_IMAGE, SOURCE_DEFINITION_ID,
            DESTINATION_DEFINITION_ID)));
  }

  @Test
  void testSourceConfigPersistError() throws Exception {
    sourceStub.setMessages(CONFIG_MESSAGE);
    final String persistErrorMessage = "there was a problem persisting the new config";
    doThrow(new RuntimeException(persistErrorMessage))
        .when(replicationAirbyteMessageEventPublishingHelper)
        .publishEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE,
            CONFIG_MESSAGE,
            new ReplicationContext(false, replicationInput.getConnectionId(), replicationInput.getSourceId(), replicationInput.getDestinationId(),
                Long.parseLong(JOB_ID), JOB_ATTEMPT, replicationInput.getWorkspaceId(), SOURCE_IMAGE, DESTINATION_IMAGE, SOURCE_DEFINITION_ID,
                DESTINATION_DEFINITION_ID)));

    final var worker = getDefaultReplicationWorker();
    doReturn(SOURCE_DEFINITION_ID).when(replicationWorkerHelper).getSourceDefinitionIdForSourceId(replicationInput.getSourceId());
    doReturn(DESTINATION_DEFINITION_ID).when(replicationWorkerHelper).getDestinationDefinitionIdForDestinationId(replicationInput.getDestinationId());

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testReplicationRunnableDestinationUpdateConfig() throws Exception {
    when(destination.attemptRead()).thenReturn(Optional.of(STATE_MESSAGE), Optional.of(CONFIG_MESSAGE));
    when(destination.isFinished()).thenReturn(false, false, true);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.COMPLETED, output.getReplicationAttemptSummary().getStatus());

    verify(replicationAirbyteMessageEventPublishingHelper).publishEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION,
        CONFIG_MESSAGE,
        new ReplicationContext(false, replicationInput.getConnectionId(), replicationInput.getSourceId(), replicationInput.getDestinationId(),
            Long.parseLong(JOB_ID), JOB_ATTEMPT, replicationInput.getWorkspaceId(), SOURCE_IMAGE, DESTINATION_IMAGE, SOURCE_DEFINITION_ID,
            DESTINATION_DEFINITION_ID)));
  }

  @Test
  void testDestinationConfigPersistError() throws Exception {
    when(destination.attemptRead()).thenReturn(Optional.of(CONFIG_MESSAGE));
    when(destination.isFinished()).thenReturn(false, true);

    final String persistErrorMessage = "there was a problem persisting the new config";
    doThrow(new RuntimeException(persistErrorMessage))
        .when(replicationAirbyteMessageEventPublishingHelper)
        .publishEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION,
            CONFIG_MESSAGE,
            new ReplicationContext(false, replicationInput.getConnectionId(), replicationInput.getSourceId(), replicationInput.getDestinationId(),
                Long.parseLong(JOB_ID), JOB_ATTEMPT, replicationInput.getWorkspaceId(), SOURCE_IMAGE, DESTINATION_IMAGE, SOURCE_DEFINITION_ID,
                DESTINATION_DEFINITION_ID)));

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testReplicationRunnableDestinationFailure() throws Exception {
    final String destinationErrorMessage = "the destination had a failure";

    doThrow(new RuntimeException(destinationErrorMessage)).when(destination).accept(any());

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.DESTINATION) && f.getStacktrace().contains(destinationErrorMessage)));
  }

  @Test
  void testReplicationRunnableDestinationFailureViaTraceMessage() throws Exception {
    final FailureReason failureReason = FailureHelper.destinationFailure(ERROR_TRACE_MESSAGE, Long.valueOf(JOB_ID), JOB_ATTEMPT);
    when(messageTracker.errorTraceMessageFailure(Long.parseLong(JOB_ID), JOB_ATTEMPT)).thenReturn(List.of(failureReason));

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.DESTINATION)
            && f.getExternalMessage().contains(ERROR_TRACE_MESSAGE.getError().getMessage())));
  }

  @Test
  void testReplicationRunnableWorkerFailure() throws Exception {
    final String workerErrorMessage = "the worker had a failure";

    doThrow(new RuntimeException(workerErrorMessage)).when(messageTracker).acceptFromSource(any());

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.REPLICATION) && f.getStacktrace().contains(workerErrorMessage)));
  }

  @Test
  void testOnlyStateAndRecordMessagesDeliveredToDestination() throws Exception {
    final AirbyteMessage logMessage = AirbyteMessageUtils.createLogMessage(Level.INFO, "a log message");
    final AirbyteMessage traceMessage = AirbyteMessageUtils.createErrorMessage("a trace message", 123456.0);
    when(mapper.mapMessage(logMessage)).thenReturn(logMessage);
    when(mapper.mapMessage(traceMessage)).thenReturn(traceMessage);
    sourceStub.setMessages(RECORD_MESSAGE1, logMessage, traceMessage, RECORD_MESSAGE2);

    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(source).start(sourceConfig, jobRoot, replicationInput.getConnectionId());
    verify(destination).start(destinationConfig, jobRoot);
    verify(destination).accept(RECORD_MESSAGE1);
    verify(destination).accept(RECORD_MESSAGE2);
    verify(destination, never()).accept(logMessage);
    verify(destination, never()).accept(traceMessage);
  }

  @Test
  void testOnlySelectedFieldsDeliveredToDestinationWithFieldSelectionEnabled() throws Exception {
    // Generate a record with an extra field.
    final AirbyteMessage recordWithExtraFields = Jsons.clone(RECORD_MESSAGE1);
    ((ObjectNode) recordWithExtraFields.getRecord().getData()).put("AnUnexpectedField", "SomeValue");
    when(mapper.mapMessage(recordWithExtraFields)).thenReturn(recordWithExtraFields);
    sourceStub.setMessages(recordWithExtraFields);
    // Use a real schema validator to make sure validation doesn't affect this.
    final String streamName = sourceConfig.getCatalog().getStreams().get(0).getStream().getName();
    final String streamNamespace = sourceConfig.getCatalog().getStreams().get(0).getStream().getNamespace();
    recordSchemaValidator = new RecordSchemaValidator(Map.of(new AirbyteStreamNameNamespacePair(streamName, streamNamespace),
        sourceConfig.getCatalog().getStreams().get(0).getStream().getJsonSchema()));
    final var worker = getDefaultReplicationWorker(true);

    worker.run(replicationInput, jobRoot);

    // Despite reading recordWithExtraFields from the source, we write the original RECORD_MESSAGE1 to
    // the destination because the new field has been filtered out.
    verify(destination).accept(RECORD_MESSAGE1);
  }

  @Test
  void testAllFieldsDeliveredWithFieldSelectionDisabled() throws Exception {
    // Generate a record with an extra field.
    final AirbyteMessage recordWithExtraFields = Jsons.clone(RECORD_MESSAGE1);
    ((ObjectNode) recordWithExtraFields.getRecord().getData()).put("AnUnexpectedField", "SomeValue");
    when(mapper.mapMessage(recordWithExtraFields)).thenReturn(recordWithExtraFields);
    sourceStub.setMessages(recordWithExtraFields);
    // Use a real schema validator to make sure validation doesn't affect this.
    final String streamName = sourceConfig.getCatalog().getStreams().get(0).getStream().getName();
    final String streamNamespace = sourceConfig.getCatalog().getStreams().get(0).getStream().getNamespace();
    recordSchemaValidator = new RecordSchemaValidator(Map.of(new AirbyteStreamNameNamespacePair(streamName, streamNamespace),
        sourceConfig.getCatalog().getStreams().get(0).getStream().getJsonSchema()));
    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    // Despite the field not being in the catalog, we write the extra field anyway because field
    // selection is disabled.
    verify(destination).accept(recordWithExtraFields);
  }

  @Test
  void testDestinationNonZeroExitValue() throws Exception {
    when(destination.getExitValue()).thenReturn(1);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream().anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.DESTINATION)));
  }

  @Test
  void testDestinationRunnableDestinationFailure() throws Exception {
    final String destinationErrorMessage = "the destination had a failure";

    when(destination.isFinished()).thenReturn(false);
    doThrow(new RuntimeException(destinationErrorMessage)).when(destination).notifyEndOfInput();

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.DESTINATION) && f.getStacktrace().contains(destinationErrorMessage)));
  }

  @Test
  @Disabled("Flaky Test")
  void testDestinationRunnableWorkerFailure() throws Exception {
    final String workerErrorMessage = "the worker had a failure";

    // Force destination to return STATE messages
    // TODO: this should come naturally from the source
    when(destination.attemptRead()).thenReturn(Optional.of(STATE_MESSAGE));

    doThrow(new RuntimeException(workerErrorMessage)).when(messageTracker).acceptFromDestination(any());

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
    assertTrue(output.getFailures().stream()
        .anyMatch(f -> f.getFailureOrigin().equals(FailureOrigin.REPLICATION) && f.getStacktrace().contains(workerErrorMessage)));
  }

  /**
   * We want to ensure logs are tested, this is to avoid duplicating setups in specific implementation
   * classes while keeping the actual checks specific as different implementation will require
   * different checks.
   */
  abstract void verifyTestLoggingInThreads(final String logs);

  @SuppressWarnings({"BusyWait"})
  @Test
  void testCancellation() throws InterruptedException {
    final AtomicReference<ReplicationOutput> output = new AtomicReference<>();
    sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1, STATE_MESSAGE);

    final var worker = getDefaultReplicationWorker();

    final Thread workerThread = new Thread(() -> {
      try {
        output.set(worker.run(replicationInput, jobRoot));
      } catch (final WorkerException e) {
        throw new RuntimeException(e);
      }
    });

    workerThread.start();

    // verify the worker is actually running before we kill it.
    while (Mockito.mockingDetails(messageTracker).getInvocations().size() < 5) {
      LOGGER.info("waiting for worker to start running");
      sleep(100);
    }

    worker.cancel();
    Assertions.assertTimeout(Duration.ofSeconds(5), (Executable) workerThread::join);
    assertNotNull(output.get());
  }

  @Test
  void testPopulatesOutputOnSuccess() throws WorkerException {
    when(syncStatsTracker.getTotalRecordsEmitted()).thenReturn(12L);
    when(syncStatsTracker.getTotalBytesFilteredOut()).thenReturn(0L);
    when(syncStatsTracker.getTotalRecordsFilteredOut()).thenReturn(0L);
    when(syncStatsTracker.getTotalBytesEmitted()).thenReturn(100L);
    when(syncStatsTracker.getTotalRecordsCommitted()).thenReturn(12L);
    when(syncStatsTracker.getTotalBytesCommitted()).thenReturn(100L);
    when(syncStatsTracker.getTotalSourceStateMessagesEmitted()).thenReturn(3L);
    when(syncStatsTracker.getTotalDestinationStateMessagesEmitted()).thenReturn(1L);
    when(syncStatsTracker.getStreamToEmittedBytes())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 100L));
    when(syncStatsTracker.getStreamToFilteredOutBytes())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 0L));
    when(syncStatsTracker.getStreamToFilteredOutRecords())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 0L));
    when(syncStatsTracker.getStreamToEmittedRecords())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 12L));
    when(syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage()).thenReturn(5L);
    when(syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage()).thenReturn(4L);
    when(syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted()).thenReturn(6L);
    when(syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted()).thenReturn(3L);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);
    // Remove performance metrics from the output, those are implementation specific and should be
    // excluded from the test.
    actual.getReplicationAttemptSummary().setPerformanceMetrics(null);

    final ReplicationOutput replicationOutput = new ReplicationOutput()
        .withReplicationAttemptSummary(new ReplicationAttemptSummary()
            .withRecordsSynced(12L)
            .withBytesSynced(100L)
            .withStatus(ReplicationStatus.COMPLETED)
            .withTotalStats(new SyncStats()
                .withRecordsEmitted(12L)
                .withBytesEmitted(100L)
                .withSourceStateMessagesEmitted(3L)
                .withDestinationStateMessagesEmitted(1L)
                .withMaxSecondsBeforeSourceStateMessageEmitted(5L)
                .withMeanSecondsBeforeSourceStateMessageEmitted(4L)
                .withBytesFilteredOut(0L)
                .withRecordsFilteredOut(0L)
                .withMaxSecondsBetweenStateMessageEmittedandCommitted(6L)
                .withMeanSecondsBetweenStateMessageEmittedandCommitted(3L)
                .withBytesCommitted(100L)
                .withRecordsCommitted(12L)) // since success, should use emitted count
            .withStreamStats(Collections.singletonList(
                new StreamSyncStats()
                    .withStreamName(STREAM1)
                    .withStreamNamespace(NAMESPACE)
                    .withStats(new SyncStats()
                        .withBytesEmitted(100L)
                        .withRecordsEmitted(12L)
                        .withRecordsFilteredOut(0L)
                        .withBytesFilteredOut(0L)
                        .withBytesCommitted(100L)
                        .withRecordsCommitted(12L) // since success, should use emitted count
                        .withSourceStateMessagesEmitted(null)
                        .withDestinationStateMessagesEmitted(null)
                        .withMaxSecondsBeforeSourceStateMessageEmitted(null)
                        .withMeanSecondsBeforeSourceStateMessageEmitted(null)
                        .withMaxSecondsBetweenStateMessageEmittedandCommitted(null)
                        .withMeanSecondsBetweenStateMessageEmittedandCommitted(null)))))
        .withOutputCatalog(replicationInput.getCatalog());

    // good enough to verify that times are present.
    assertNotNull(actual.getReplicationAttemptSummary().getStartTime());
    assertNotNull(actual.getReplicationAttemptSummary().getEndTime());

    // verify output object matches declared json schema spec.
    final Set<String> validate = new JsonSchemaValidator()
        .validate(Jsons.jsonNode(Jsons.jsonNode(JsonSchemaValidator.getSchema(ConfigSchema.REPLICATION_OUTPUT.getConfigSchemaFile()))),
            Jsons.jsonNode(actual));
    assertTrue(validate.isEmpty(), "Validation errors: " + Strings.join(validate, ","));

    // remove times, so we can do the rest of the object <> object comparison.
    actual.getReplicationAttemptSummary().withStartTime(null).withEndTime(null).getTotalStats().withReplicationStartTime(null)
        .withReplicationEndTime(null)
        .withSourceReadStartTime(null).withSourceReadEndTime(null)
        .withDestinationWriteStartTime(null).withDestinationWriteEndTime(null);

    assertEquals(replicationOutput, actual);
  }

  @Test
  void testPopulatesStatsOnFailureIfAvailable() throws Exception {
    doThrow(new IllegalStateException(INDUCED_EXCEPTION)).when(source).close();
    when(syncStatsTracker.getTotalRecordsEmitted()).thenReturn(12L);
    when(syncStatsTracker.getTotalRecordsFilteredOut()).thenReturn(0L);
    when(syncStatsTracker.getTotalBytesFilteredOut()).thenReturn(0L);
    when(syncStatsTracker.getTotalBytesEmitted()).thenReturn(100L);
    when(syncStatsTracker.getTotalBytesCommitted()).thenReturn(12L);
    when(syncStatsTracker.getTotalRecordsCommitted()).thenReturn(6L);
    when(syncStatsTracker.getTotalSourceStateMessagesEmitted()).thenReturn(3L);
    when(syncStatsTracker.getTotalDestinationStateMessagesEmitted()).thenReturn(2L);
    when(syncStatsTracker.getStreamToEmittedBytes())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 100L));
    when(syncStatsTracker.getStreamToFilteredOutBytes())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 0L));
    when(syncStatsTracker.getStreamToFilteredOutRecords())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 0L));
    when(syncStatsTracker.getStreamToEmittedRecords())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 12L));
    when(syncStatsTracker.getStreamToCommittedRecords())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 6L));
    when(syncStatsTracker.getStreamToCommittedBytes())
        .thenReturn(Collections.singletonMap(new AirbyteStreamNameNamespacePair(STREAM1, NAMESPACE), 13L));
    when(syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage()).thenReturn(10L);
    when(syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage()).thenReturn(8L);
    when(syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted()).thenReturn(12L);
    when(syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted()).thenReturn(11L);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);
    final SyncStats expectedTotalStats = new SyncStats()
        .withRecordsEmitted(12L)
        .withBytesEmitted(100L)
        .withSourceStateMessagesEmitted(3L)
        .withDestinationStateMessagesEmitted(2L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(10L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(8L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(12L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(11L)
        .withBytesCommitted(12L)
        .withBytesFilteredOut(0L)
        .withRecordsFilteredOut(0L)
        .withRecordsCommitted(6L);
    final List<StreamSyncStats> expectedStreamStats = Collections.singletonList(
        new StreamSyncStats()
            .withStreamName(STREAM1)
            .withStreamNamespace(NAMESPACE)
            .withStats(new SyncStats()
                .withBytesEmitted(100L)
                .withRecordsEmitted(12L)
                .withBytesCommitted(13L)
                .withBytesFilteredOut(0L)
                .withRecordsFilteredOut(0L)
                .withRecordsCommitted(6L)
                .withSourceStateMessagesEmitted(null)
                .withDestinationStateMessagesEmitted(null)));

    assertNotNull(actual);
    // null out timing stats for assertion matching
    assertEquals(expectedTotalStats, actual.getReplicationAttemptSummary().getTotalStats().withReplicationStartTime(null).withReplicationEndTime(null)
        .withSourceReadStartTime(null).withSourceReadEndTime(null).withDestinationWriteStartTime(null).withDestinationWriteEndTime(null));
    assertEquals(expectedStreamStats, actual.getReplicationAttemptSummary().getStreamStats());
  }

  @Test
  void testDoesNotPopulatesStateOnFailureIfNotAvailable() throws Exception {
    final ReplicationInput replicationInputWithoutState = Jsons.clone(replicationInput);
    replicationInputWithoutState.setState(null);

    doThrow(new IllegalStateException(INDUCED_EXCEPTION)).when(source).close();

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInputWithoutState, jobRoot);

    assertNotNull(actual);
    assertNull(actual.getState());
  }

  @Test
  void testDoesNotPopulateOnIrrecoverableFailure() {
    doThrow(new IllegalStateException(INDUCED_EXCEPTION)).when(syncStatsTracker).getTotalRecordsEmitted();

    final var worker = getDefaultReplicationWorker();
    assertThrows(WorkerException.class, () -> worker.run(replicationInput, jobRoot));
  }

  @Test
  void testSourceFailingTimeout() throws Exception {
    final HeartbeatMonitor heartbeatMonitor = mock(HeartbeatMonitor.class);
    when(heartbeatMonitor.isBeating()).thenReturn(Optional.of(false));
    final UUID connectionId = UUID.randomUUID();
    final MetricClient mMetricClient = mock(MetricClient.class);
    heartbeatTimeoutChaperone =
        new HeartbeatTimeoutChaperone(heartbeatMonitor, Duration.ofMillis(1), new TestClient(Map.of("heartbeat.failSync", true)), UUID.randomUUID(),
            connectionId, "docker image", mMetricClient);
    sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);

    verify(mMetricClient).count(OssMetricsRegistry.SOURCE_HEARTBEAT_FAILURE, 1,
        new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        new MetricAttribute(MetricTags.KILLED, "true"),
        new MetricAttribute(MetricTags.SOURCE_IMAGE, "docker image"));
    assertEquals(1, actual.getFailures().size());
    assertEquals(FailureOrigin.SOURCE, actual.getFailures().get(0).getFailureOrigin());
    assertEquals(FailureReason.FailureType.HEARTBEAT_TIMEOUT, actual.getFailures().get(0).getFailureType());
  }

  @Test
  void testDestinationAcceptTimeout() throws Exception {
    when(replicationFeatureFlagReader.readReplicationFeatureFlags())
        .thenReturn(new ReplicationFeatureFlags(true, 0, 4, false, false, false));

    destinationTimeoutMonitor = spy(new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        true,
        Duration.ofSeconds(1)));

    destination = new SimpleTimeoutMonitoredDestination(destinationTimeoutMonitor);

    final AtomicBoolean acceptCallIsStuck = new AtomicBoolean(true);
    doAnswer(invocation -> {
      // replication is hanging on accept call
      while (acceptCallIsStuck.get()) {
        Thread.sleep(1000);
      }
      return null;
    }).when(destinationTimeoutMonitor).resetAcceptTimer();

    doAnswer(invocation -> {
      try {
        invocation.callRealMethod();
      } finally {
        // replication stops hanging on accept call
        acceptCallIsStuck.set(false);
      }
      return null;
    }).when(destinationTimeoutMonitor).runWithTimeoutThread(any());

    sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1);

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);

    verify(metricClient).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), eq(1L), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), anyLong(), any(MetricAttribute.class));

    assertEquals(1, actual.getFailures().size());
    assertEquals(FailureOrigin.DESTINATION, actual.getFailures().get(0).getFailureOrigin());
    assertEquals(FailureReason.FailureType.DESTINATION_TIMEOUT, actual.getFailures().get(0).getFailureType());
  }

  @Test
  void testDestinationNotifyEndOfInputTimeout() throws Exception {
    when(replicationFeatureFlagReader.readReplicationFeatureFlags())
        .thenReturn(new ReplicationFeatureFlags(true, 0, 4, false, false, false));

    destinationTimeoutMonitor = spy(new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        true,
        Duration.ofSeconds(1)));

    destination = new SimpleTimeoutMonitoredDestination(destinationTimeoutMonitor);

    final AtomicBoolean notifyEndOfInputCallIsStuck = new AtomicBoolean(true);
    doAnswer(invocation -> {
      // replication is hanging on notifyEndOfInput call
      while (notifyEndOfInputCallIsStuck.get()) {
        Thread.sleep(1000);
      }
      return null;
    }).when(destinationTimeoutMonitor).resetNotifyEndOfInputTimer();

    doAnswer(invocation -> {
      try {
        invocation.callRealMethod();
      } finally {
        // replication stops hanging on notifyEndOfInput call
        notifyEndOfInputCallIsStuck.set(false);
      }
      return null;
    }).when(destinationTimeoutMonitor).runWithTimeoutThread(any());

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);

    verify(metricClient).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), eq(1L), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), anyLong(), any(MetricAttribute.class));

    assertEquals(1, actual.getFailures().size());
    assertEquals(FailureOrigin.DESTINATION, actual.getFailures().get(0).getFailureOrigin());
    assertEquals(FailureReason.FailureType.DESTINATION_TIMEOUT, actual.getFailures().get(0).getFailureType());
  }

  @Test
  void testDestinationTimeoutWithCloseFailure() throws Exception {
    when(replicationFeatureFlagReader.readReplicationFeatureFlags())
        .thenReturn(new ReplicationFeatureFlags(true, 0, 4, false, false, false));

    destinationTimeoutMonitor = spy(new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        true,
        Duration.ofSeconds(1)));

    destination = spy(new SimpleTimeoutMonitoredDestination(destinationTimeoutMonitor));

    doAnswer(invocation -> {
      throw new RuntimeException("close failed");
    })
        .when(destination).close();

    final AtomicBoolean notifyEndOfInputCallIsStuck = new AtomicBoolean(true);
    doAnswer(invocation -> {
      // replication is hanging on notifyEndOfInput call
      while (notifyEndOfInputCallIsStuck.get()) {
        Thread.sleep(1000);
      }
      return null;
    }).when(destinationTimeoutMonitor).resetNotifyEndOfInputTimer();

    doAnswer(invocation -> {
      try {
        invocation.callRealMethod();
      } finally {
        // replication stops hanging on notifyEndOfInput call
        notifyEndOfInputCallIsStuck.set(false);
      }
      return null;
    }).when(destinationTimeoutMonitor).runWithTimeoutThread(any());

    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput actual = worker.run(replicationInput, jobRoot);

    verify(metricClient).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), eq(1L), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), anyLong(), any(MetricAttribute.class));

    assertEquals(FailureOrigin.DESTINATION, actual.getFailures().get(0).getFailureOrigin());
    assertEquals(FailureReason.FailureType.DESTINATION_TIMEOUT, actual.getFailures().get(0).getFailureType());
  }

  @Test
  void testGetFailureReason() {
    final long jobId = 1;
    final int attempt = 1;
    FailureReason failureReason = ReplicationWorkerHelper.getFailureReason(new SourceException(""), jobId, attempt);
    assertEquals(failureReason.getFailureOrigin(), FailureOrigin.SOURCE);
    failureReason = ReplicationWorkerHelper.getFailureReason(new DestinationException(""), jobId, attempt);
    assertEquals(failureReason.getFailureOrigin(), FailureOrigin.DESTINATION);
    failureReason = ReplicationWorkerHelper.getFailureReason(new HeartbeatTimeoutChaperone.HeartbeatTimeoutException(10000, 15000), jobId, attempt);
    assertEquals(failureReason.getFailureOrigin(), FailureOrigin.SOURCE);
    assertEquals(failureReason.getFailureType(), FailureReason.FailureType.HEARTBEAT_TIMEOUT);
    assertEquals(
        "Airbyte detected that the Source didn't send any records in the last 15 seconds, exceeding the configured 10 seconds threshold. Airbyte will try reading again on the next sync. Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info.",
        failureReason.getExternalMessage());
    assertEquals("Last record seen 15 seconds ago, exceeding the threshold of 10 seconds.", failureReason.getInternalMessage());
    failureReason = ReplicationWorkerHelper.getFailureReason(new RuntimeException(), jobId, attempt);
    assertEquals(failureReason.getFailureOrigin(), FailureOrigin.REPLICATION);
    failureReason = ReplicationWorkerHelper.getFailureReason(new TimeoutException(10000, 15000), jobId, attempt);
    assertEquals(
        "Airbyte detected that the Destination didn't make progress in the last 15 seconds, exceeding the configured 10 seconds threshold. Airbyte will try reading again on the next sync. Please see https://docs.airbyte.com/understanding-airbyte/heartbeats for more info.",
        failureReason.getExternalMessage());
    assertEquals("Last action 15 seconds ago, exceeding the threshold of 10 seconds.", failureReason.getInternalMessage());
    assertEquals(failureReason.getFailureOrigin(), FailureOrigin.DESTINATION);
    assertEquals(failureReason.getFailureType(), FailureType.DESTINATION_TIMEOUT);
  }

  @Test
  void testCallHeartbeat() throws WorkerException {
    sourceStub.setMessages(RECORD_MESSAGE1);

    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(replicationWorkerHelper).getWorkloadStatusHeartbeat(any());
  }

  @Test
  void testStreamStatusCompletionTracking() throws Exception {
    sourceStub.setMessages(RECORD_MESSAGE1);

    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(streamStatusCompletionTracker).startTracking(any(), anyBoolean());

    verify(streamStatusCompletionTracker).finalize(0, mapper);
  }

  @Test
  void testStreamStatusCompletionTrackingTrackSourceMessage() throws Exception {

    final AirbyteMessage streamStatus = AirbyteMessageUtils.createStatusTraceMessage(new StreamDescriptor().withName(STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE);
    sourceStub.setMessages(RECORD_MESSAGE1, streamStatus);

    final var worker = getDefaultReplicationWorker();

    worker.run(replicationInput, jobRoot);

    verify(streamStatusCompletionTracker).startTracking(any(), anyBoolean());
    verify(streamStatusCompletionTracker).track(streamStatus.getTrace().getStreamStatus());
    verify(streamStatusCompletionTracker).finalize(0, mapper);
  }

}
