/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.performance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.protocol.AirbyteMessageMigrator;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator;
import io.airbyte.commons.version.Version;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.lib.NotImplementedMetricClient;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.EmptyAirbyteDestination;
import io.airbyte.workers.general.LimitedFatRecordSourceProcess;
import io.airbyte.workers.general.LimitedIntegrationLauncher;
import io.airbyte.workers.general.ReplicationFeatureFlagReader;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerHelper;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.DefaultAirbyteSource;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.StreamStatusTracker;
import io.airbyte.workers.internal.bookkeeping.events.AirbyteControlMessageEventListener;
import io.airbyte.workers.internal.bookkeeping.events.AirbyteStreamStatusMessageEventListener;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.workload.WorkloadIdGenerator;
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
                                                         final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                                         final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                         final ReplicationAirbyteMessageEventPublishingHelper messageEventPublishingHelper,
                                                         final ReplicationWorkerHelper replicationWorkerHelper,
                                                         final DestinationTimeoutMonitor destinationTimeoutMonitor);

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
    final var messageTracker = mock(AirbyteMessageTracker.class);
    final var analyticsMessageTracker = mock(AnalyticsMessageTracker.class);
    final var syncPersistence = mock(SyncPersistence.class);
    final var connectorConfigUpdater = mock(ConnectorConfigUpdater.class);
    final var metricReporter = new WorkerMetricReporter(new NotImplementedMetricClient(), "test-image:0.01");
    final var dstNamespaceMapper = new NamespacingMapper(NamespaceDefinitionType.DESTINATION, "", "");
    final var validator = new RecordSchemaValidator(Map.of(
        new AirbyteStreamNameNamespacePair("s1", null),
        CatalogHelpers.fieldsToJsonSchema(io.airbyte.protocol.models.Field.of("data", JsonSchemaType.STRING))));
    final var airbyteMessageDataExtractor = new AirbyteMessageDataExtractor();
    final var replicationFeatureFlagReader = mock(ReplicationFeatureFlagReader.class);
    when(replicationFeatureFlagReader.readReplicationFeatureFlags()).thenReturn(new ReplicationFeatureFlags(false, 0, 4));

    // final IntegrationLauncher integrationLauncher = new LimitedIntegrationLauncher(new
    // LimitedThinRecordSourceProcess());
    final IntegrationLauncher integrationLauncher = new LimitedIntegrationLauncher(new LimitedFatRecordSourceProcess());

    final var msgMigrator = new AirbyteMessageMigrator(List.of());
    msgMigrator.initialize();
    final ConfiguredAirbyteCatalogMigrator catalogMigrator = new ConfiguredAirbyteCatalogMigrator(List.of());
    catalogMigrator.initialize();
    final var migratorFactory = new AirbyteProtocolVersionedMigratorFactory(msgMigrator, catalogMigrator);

    final var versionFac = VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(false);
    final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(DEFAULT_HEARTBEAT_FRESHNESS_THRESHOLD);
    final var versionedAbSource =
        new DefaultAirbyteSource(integrationLauncher, versionFac, heartbeatMonitor, migratorFactory.getProtocolSerializer(new Version("0.2.0")),
            new EnvVariableFeatureFlags());
    final var workspaceID = UUID.randomUUID();
    final FeatureFlagClient featureFlagClient = new TestClient(Map.of("heartbeat.failSync", false));
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(heartbeatMonitor,
        io.airbyte.workers.internal.HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
        featureFlagClient,
        workspaceID,
        UUID.randomUUID(),
        new NotImplementedMetricClient());
    final StreamStatusTracker streamStatusTracker = new StreamStatusTracker(mock(AirbyteApiClient.class));
    final List<ApplicationEventListener<ReplicationAirbyteMessageEvent>> listeners = List.of(
        new AirbyteControlMessageEventListener(connectorConfigUpdater),
        new AirbyteStreamStatusMessageEventListener(streamStatusTracker));
    final DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        workspaceID,
        UUID.randomUUID(),
        new NotImplementedMetricClient(),
        Duration.ofMinutes(120),
        false);
    final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper =
        mock(ReplicationAirbyteMessageEventPublishingHelper.class);
    doAnswer((e) -> {
      final ReplicationAirbyteMessageEvent event = e.getArgument(0);
      listeners.forEach(l -> l.onApplicationEvent(event));
      return null;
    }).when(replicationAirbyteMessageEventPublishingHelper).publishStatusEvent(any(ReplicationAirbyteMessageEvent.class));

    final boolean fieldSelectionEnabled = false;
    final FieldSelector fieldSelector = new FieldSelector(validator, metricReporter, fieldSelectionEnabled, false);

    final ReplicationWorkerHelper replicationWorkerHelper =
        new ReplicationWorkerHelper(airbyteMessageDataExtractor, fieldSelector, dstNamespaceMapper, messageTracker, syncPersistence,
            replicationAirbyteMessageEventPublishingHelper, new ThreadedTimeTracker(), () -> {}, mock(WorkloadApi.class),
            new WorkloadIdGenerator(), false, analyticsMessageTracker);

    final var worker = getReplicationWorker("1", 0,
        versionedAbSource,
        dstNamespaceMapper,
        perDestination,
        messageTracker,
        syncPersistence,
        validator,
        fieldSelector,
        heartbeatTimeoutChaperone,
        replicationFeatureFlagReader,
        airbyteMessageDataExtractor,
        replicationAirbyteMessageEventPublishingHelper,
        replicationWorkerHelper,
        destinationTimeoutMonitor);
    final AtomicReference<ReplicationOutput> output = new AtomicReference<>();
    final Thread workerThread = new Thread(() -> {
      try {
        final var ignoredPath = Path.of("/");
        final ReplicationInput testInput = new ReplicationInput().withCatalog(
            // The stream fields here are intended to match the records emitted by the
            // LimitedFatRecordSourceProcess
            // class.
            CatalogHelpers.createConfiguredAirbyteCatalog("s1", null, Field.of("data", JsonSchemaType.STRING)))
            .withWorkspaceId(UUID.randomUUID());
        output.set(worker.run(testInput, ignoredPath));
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
