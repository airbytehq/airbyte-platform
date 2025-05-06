/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.performance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.protocol.AirbyteMessageMigrator;
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.container.orchestrator.tracker.ThreadedTimeTracker;
import io.airbyte.container.orchestrator.worker.EmptyAirbyteDestination;
import io.airbyte.container.orchestrator.worker.LimitedFatRecordSourceProcess;
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider;
import io.airbyte.container.orchestrator.worker.ReplicationWorker;
import io.airbyte.container.orchestrator.worker.ReplicationWorkerHelper;
import io.airbyte.container.orchestrator.worker.ReplicationWorkerState;
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor;
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteSource;
import io.airbyte.featureflag.DestinationTimeoutEnabled;
import io.airbyte.featureflag.DestinationTimeoutSeconds;
import io.airbyte.featureflag.FailSyncOnInvalidChecksum;
import io.airbyte.featureflag.LogConnectorMessages;
import io.airbyte.featureflag.LogStateMsgs;
import io.airbyte.featureflag.ShouldFailSyncIfHeartbeatFailure;
import io.airbyte.featureflag.WorkloadHeartbeatRate;
import io.airbyte.featureflag.WorkloadHeartbeatTimeout;
import io.airbyte.mappers.application.RecordMapper;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.metrics.MetricClient;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.AirbyteControlMessageEventListener;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.micronaut.context.event.ApplicationEventListener;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReplicationWorkerPerformanceTest {

  private static final Logger log = LoggerFactory.getLogger(ReplicationWorkerPerformanceTest.class);

  public static final Duration DEFAULT_HEARTBEAT_FRESHNESS_THRESHOLD = Duration.ofMillis(1);
  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());

  public abstract ReplicationWorker getReplicationWorker(final String jobId,
                                                         final int attempt,
                                                         final AirbyteSource source,
                                                         final AirbyteMapper mapper,
                                                         final AirbyteDestination destination,
                                                         final AirbyteMessageTracker messageTracker,
                                                         final SyncPersistence syncPersistence,
                                                         final RecordSchemaValidator recordSchemaValidator,
                                                         final FieldSelector fieldSelector,
                                                         final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                                         final ReplicationInputFeatureFlagReader replicationInputFeatureFlagReader,
                                                         final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                         final ReplicationAirbyteMessageEventPublishingHelper messageEventPublishingHelper,
                                                         final ReplicationWorkerHelper replicationWorkerHelper,
                                                         final DestinationTimeoutMonitor destinationTimeoutMonitor,
                                                         final StreamStatusCompletionTracker streamStatusCompletionTracker,
                                                         final MetricClient metricClient,
                                                         final ReplicationInput replicationInput);

  /**
   * Hook up the DefaultReplicationWorker to a test harness with an insanely quick Source
   * {@link LimitedFatRecordSourceProcess} via the {@link LimitedIntegrationLauncher} and Destination
   * {@link EmptyAirbyteDestination}.
   * <p>
   * Harness uses Java Micro Benchmark to run the E2E sync a configured number of times. It then
   * reports a time distribution for the time taken to run the E2E sync.
   * <p>
   * Because the reported time does not explicitly include throughput numbers, throughput logging has
   * been added. This class is intended to help devs understand the impact of changes on throughput.
   * <p>
   * To use this, simply run the main method, make yourself a cup of coffee for 5 mins, then look the
   * logs.
   */
  // @Benchmark
  // SampleTime = the time taken to run the benchmarked method. Use this because we only care about
  // the time taken to sync the entire dataset.
  // @BenchmarkMode(Mode.SampleTime)
  // Warming up the JVM stabilises results however takes longer. Skip this for now since we don't need
  // that fine a result.
  // @Warmup(iterations = 0)
  // How many runs to do.
  // @Fork(1)
  // Within each run, how many iterations to do.
  // @Measurement(iterations = 2)
  public void executeOneSync() throws InterruptedException {
    log.warn("availableProcessors {}", Runtime.getRuntime().availableProcessors());
    final var perDestination = new EmptyAirbyteDestination();
    final var metricClient = mock(MetricClient.class);
    final var messageTracker = mock(AirbyteMessageTracker.class);
    final var analyticsMessageTracker = mock(AnalyticsMessageTracker.class);
    final var syncPersistence = mock(SyncPersistence.class);
    final var connectorConfigUpdater = mock(ConnectorConfigUpdater.class);
    final var metricReporter = new WorkerMetricReporter(metricClient, "test-image:0.01");
    final var dstNamespaceMapper = new NamespacingMapper(NamespaceDefinitionType.DESTINATION, "", "");
    final var validator = new RecordSchemaValidator(Map.of(
        new AirbyteStreamNameNamespacePair("s1", null),
        CatalogHelpers.fieldsToJsonSchema(io.airbyte.protocol.models.Field.of("data", JsonSchemaType.STRING))));
    final var airbyteMessageDataExtractor = new AirbyteMessageDataExtractor();
    final var replicationInputFeatureFlagReader = mock(ReplicationInputFeatureFlagReader.class);
    final var replicationInput = mock(ReplicationInput.class);
    when(replicationInputFeatureFlagReader.read(DestinationTimeoutEnabled.INSTANCE))
        .thenReturn(true);
    when(replicationInputFeatureFlagReader.read(WorkloadHeartbeatRate.INSTANCE))
        .thenReturn(0);
    when(replicationInputFeatureFlagReader.read(WorkloadHeartbeatTimeout.INSTANCE))
        .thenReturn(4);
    when(replicationInputFeatureFlagReader.read(FailSyncOnInvalidChecksum.INSTANCE))
        .thenReturn(false);
    when(replicationInputFeatureFlagReader.read(LogStateMsgs.INSTANCE))
        .thenReturn(false);
    when(replicationInputFeatureFlagReader.read(LogConnectorMessages.INSTANCE))
        .thenReturn(false);
    when(replicationInputFeatureFlagReader.read(DestinationTimeoutSeconds.INSTANCE))
        .thenReturn(7200);
    when(replicationInputFeatureFlagReader.read(ShouldFailSyncIfHeartbeatFailure.INSTANCE))
        .thenReturn(false);

    final var msgMigrator = new AirbyteMessageMigrator(List.of());
    msgMigrator.initialize();
    final ConfiguredAirbyteCatalogMigrator catalogMigrator = new ConfiguredAirbyteCatalogMigrator(List.of());
    catalogMigrator.initialize();

    final var versionFac = VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(metricClient);
    final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(DEFAULT_HEARTBEAT_FRESHNESS_THRESHOLD);
    // TODO: This needs to be fixed to pass a NOOP tracker and a proper container IO handle
    final var versionedAbSource = new LocalContainerAirbyteSource(heartbeatMonitor, versionFac, null, null);
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(heartbeatMonitor,
        io.airbyte.workers.internal.HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
        replicationInputFeatureFlagReader,
        UUID.randomUUID(),
        "docker image",
        metricClient);
    final List<ApplicationEventListener<ReplicationAirbyteMessageEvent>> listeners = List.of(
        new AirbyteControlMessageEventListener(connectorConfigUpdater));
    final DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        replicationInput,
        replicationInputFeatureFlagReader,
        metricClient,
        Duration.ofMinutes(120));
    final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper =
        mock(ReplicationAirbyteMessageEventPublishingHelper.class);
    doAnswer((e) -> {
      final ReplicationAirbyteMessageEvent event = e.getArgument(0);
      listeners.forEach(l -> l.onApplicationEvent(event));
      return null;
    }).when(replicationAirbyteMessageEventPublishingHelper).publishEvent(any(ReplicationAirbyteMessageEvent.class));

    final boolean fieldSelectionEnabled = false;
    final FieldSelector fieldSelector = new FieldSelector(validator, metricReporter, fieldSelectionEnabled, false);
    final WorkloadApiClient workloadApiClient = mock(WorkloadApiClient.class);
    when(workloadApiClient.getWorkloadApi()).thenReturn(mock(WorkloadApi.class));
    final AirbyteApiClient airbyteApiClient = mock(AirbyteApiClient.class);
    when(airbyteApiClient.getDestinationApi()).thenReturn(mock(DestinationApi.class));
    when(airbyteApiClient.getSourceApi()).thenReturn(mock(SourceApi.class));

    final StreamStatusTracker streamStatusTracker = mock(StreamStatusTracker.class);

    final RecordMapper recordMapper = mock(RecordMapper.class);

    final ReplicationWorkerState replicationWorkerState = new ReplicationWorkerState();
    final ReplicationContextProvider.Context replicationContext = mock(ReplicationContextProvider.Context.class);

    final ReplicationWorkerHelper replicationWorkerHelper =
        new ReplicationWorkerHelper(
            fieldSelector,
            dstNamespaceMapper,
            messageTracker,
            syncPersistence,
            replicationAirbyteMessageEventPublishingHelper,
            new ThreadedTimeTracker(),
            analyticsMessageTracker,
            mock(StreamStatusCompletionTracker.class),
            streamStatusTracker,
            recordMapper,
            replicationWorkerState,
            replicationContext,
            mock(DestinationCatalogGenerator.class),
            metricClient);
    final StreamStatusCompletionTracker streamStatusCompletionTracker = mock(StreamStatusCompletionTracker.class);

    final var worker = getReplicationWorker("1", 0,
        versionedAbSource,
        dstNamespaceMapper,
        perDestination,
        messageTracker,
        syncPersistence,
        validator,
        fieldSelector,
        heartbeatTimeoutChaperone,
        replicationInputFeatureFlagReader,
        airbyteMessageDataExtractor,
        replicationAirbyteMessageEventPublishingHelper,
        replicationWorkerHelper,
        destinationTimeoutMonitor,
        streamStatusCompletionTracker,
        metricClient,
        replicationInput);
    final AtomicReference<ReplicationOutput> output = new AtomicReference<>();
    final Thread workerThread = new Thread(() -> {
      final var ignoredPath = Path.of("/");
      final ReplicationInput testInput = new ReplicationInput().withCatalog(
          // The stream fields here are intended to match the records emitted by the
          // LimitedFatRecordSourceProcess
          // class.
          catalogHelpers.createConfiguredAirbyteCatalog("s1", null, Field.of("data", JsonSchemaType.STRING)))
          .withWorkspaceId(UUID.randomUUID());
      try {
        final ReplicationOutput replicationOutput = worker.runReplicationBlocking(testInput, ignoredPath);
        output.set(replicationOutput);
      } catch (final WorkerException e) {
        throw new RuntimeException(e);
      }
    });

    workerThread.start();
    workerThread.join();
    final var summary = output.get().getReplicationAttemptSummary();
    final var mbRead = summary.getBytesSynced() / 1_000_000;
    final var timeTakenMs = (summary.getEndTime() - summary.getStartTime());
    final var timeTakenSec = timeTakenMs / 1000.0;
    final var recReadSec = summary.getRecordsSynced() / timeTakenSec;
    log.info("MBs read: {}, Time taken sec: {}, MB/s: {}, records/s: {}", mbRead, timeTakenSec, mbRead / timeTakenSec, recReadSec);
  }

}
