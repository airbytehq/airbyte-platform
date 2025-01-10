/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.GlobalTracer;
import datadog.trace.api.Tracer;
import io.airbyte.commons.temporal.TemporalInitializationUtils;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.config.TemporalQueueConfiguration;
import io.airbyte.config.MaxWorkersConfig;
import io.airbyte.micronaut.temporal.TemporalProxyHelper;
import io.airbyte.workers.temporal.check.connection.CheckConnectionWorkflowImpl;
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogWorkflowImpl;
import io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowImpl;
import io.airbyte.workers.temporal.spec.SpecWorkflowImpl;
import io.airbyte.workers.temporal.sync.SyncWorkflowImpl;
import io.airbyte.workers.temporal.workflows.ConnectorCommandWorkflowImpl;
import io.airbyte.workers.temporal.workflows.DiscoverCatalogAndAutoPropagateWorkflowImpl;
import io.airbyte.workers.tracing.StorageObjectGetInterceptor;
import io.airbyte.workers.tracing.TemporalSdkInterceptor;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import io.micronaut.discovery.event.ServiceReadyEvent;
import io.micronaut.scheduling.TaskExecutors;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.NonDeterministicException;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.WorkflowImplementationOptions;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs any required initialization logic on application context start.
 */
@Singleton
@Requires(notEnv = {Environment.TEST})
public class ApplicationInitializer implements ApplicationEventListener<ServiceReadyEvent> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  @Named("uiCommandsActivities")
  private Optional<List<Object>> uiCommandsActivities;

  @Inject
  @Named("connectionManagerActivities")
  private Optional<List<Object>> connectionManagerActivities;

  @Inject
  @Named(TaskExecutors.IO)
  private ExecutorService executorService;

  @Value("${airbyte.worker.check.max-workers}")
  private Integer maxCheckWorkers;
  @Value("${airbyte.worker.notify.max-workers}")
  private Integer maxNotifyWorkers;
  @Value("${airbyte.worker.discover.max-workers}")
  private Integer maxDiscoverWorkers;
  @Value("${airbyte.worker.spec.max-workers}")
  private Integer maxSpecWorkers;
  @Value("${airbyte.worker.sync.max-workers}")
  private Integer maxSyncWorkers;
  @Value("${airbyte.worker.check.enabled}")
  private boolean shouldRunCheckConnectionWorkflows;
  @Value("${airbyte.worker.connection.enabled}")
  private boolean shouldRunConnectionManagerWorkflows;
  @Value("${airbyte.worker.discover.enabled}")
  private boolean shouldRunDiscoverWorkflows;
  @Value("${airbyte.worker.spec.enabled}")
  private boolean shouldRunGetSpecWorkflows;
  @Value("${airbyte.worker.sync.enabled}")
  private boolean shouldRunSyncWorkflows;

  @Inject
  @Named("syncActivities")
  private Optional<List<Object>> syncActivities;
  @Inject
  private TemporalInitializationUtils temporalInitializationUtils;
  @Inject
  private TemporalProxyHelper temporalProxyHelper;
  @Inject
  private WorkflowServiceStubs temporalService;
  @Inject
  private TemporalUtils temporalUtils;
  @Inject
  private WorkerFactory workerFactory;

  @Inject
  private TemporalQueueConfiguration temporalQueueConfiguration;

  @Value("${airbyte.data.sync.task-queue}")
  private String syncTaskQueue;

  @Value("${airbyte.data.check.task-queue}")
  private String checkTaskQueue;

  @Value("${airbyte.data.discover.task-queue}")
  private String discoverTaskQueue;

  @Override
  public void onApplicationEvent(final ServiceReadyEvent event) {
    try {
      configureTracer();
      initializeCommonDependencies();

      registerWorkerFactory(workerFactory,
          new MaxWorkersConfig(maxCheckWorkers, maxDiscoverWorkers, maxSpecWorkers,
              maxSyncWorkers, maxNotifyWorkers));

      log.info("Starting worker factory...");
      workerFactory.start();

      log.info("Application initialized.");
    } catch (final ExecutionException | InterruptedException | TimeoutException e) {
      log.error("Unable to initialize application.", e);
      throw new IllegalStateException(e);
    }
  }

  private void configureTracer() {
    final Tracer globalTracer = GlobalTracer.get();
    globalTracer.addTraceInterceptor(new StorageObjectGetInterceptor());
    globalTracer.addTraceInterceptor(new TemporalSdkInterceptor());
  }

  private void initializeCommonDependencies()
      throws ExecutionException, InterruptedException, TimeoutException {
    log.info("Initializing common worker dependencies.");

    configureTemporal(temporalUtils, temporalService);
  }

  private void registerWorkerFactory(final WorkerFactory workerFactory,
                                     final MaxWorkersConfig maxWorkersConfiguration) {
    log.info("Registering worker factories....");
    registerUiCommandsWorker(workerFactory, maxWorkersConfiguration);

    if (shouldRunSyncWorkflows) {
      registerSync(workerFactory, maxWorkersConfiguration);
    }

    if (shouldRunConnectionManagerWorkflows) {
      registerConnectionManager(workerFactory, maxWorkersConfiguration);
    }

    if (shouldRunGetSpecWorkflows) {
      registerGetSpec(workerFactory);
    }

    if (shouldRunCheckConnectionWorkflows) {
      registerCheckConnection(workerFactory);
    }

    if (shouldRunDiscoverWorkflows) {
      registerDiscover(workerFactory);
    }
  }

  private void registerUiCommandsWorker(final WorkerFactory factory, final MaxWorkersConfig maxWorkersConfiguration) {
    final Worker uiCommandsWorker =
        factory.newWorker(temporalQueueConfiguration.getUiCommandsQueue(), getWorkerOptions(maxWorkersConfiguration.getMaxCheckWorkers()));
    final WorkflowImplementationOptions workflowOptions = WorkflowImplementationOptions.newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();

    uiCommandsWorker.registerWorkflowImplementationTypes(
        workflowOptions,
        temporalProxyHelper.proxyWorkflowClass(ConnectorCommandWorkflowImpl.class));
    uiCommandsWorker.registerActivitiesImplementations(uiCommandsActivities.orElseThrow().toArray(new Object[] {}));
    log.info("UI Commands Worker registered.");
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated
  private void registerCheckConnection(final WorkerFactory factory) {
    final Set<String> taskQueues = getCheckTaskQueue();
    for (final String taskQueue : taskQueues) {
      final Worker checkConnectionWorker = factory.newWorker(taskQueue, getWorkerOptions(1));
      final WorkflowImplementationOptions options = WorkflowImplementationOptions.newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();
      checkConnectionWorker
          .registerWorkflowImplementationTypes(options,
              temporalProxyHelper.proxyWorkflowClass(CheckConnectionWorkflowImpl.class));
      log.info("Check Connection Workflow registered.");
    }
  }

  private void registerConnectionManager(final WorkerFactory factory,
                                         final MaxWorkersConfig maxWorkersConfig) {
    final Worker connectionUpdaterWorker =
        factory.newWorker(TemporalJobType.CONNECTION_UPDATER.toString(),
            getWorkerOptions(maxWorkersConfig.getMaxSyncWorkers()));
    final WorkflowImplementationOptions options = WorkflowImplementationOptions.newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();
    connectionUpdaterWorker
        .registerWorkflowImplementationTypes(options,
            temporalProxyHelper.proxyWorkflowClass(ConnectionManagerWorkflowImpl.class));
    connectionUpdaterWorker.registerActivitiesImplementations(
        connectionManagerActivities.orElseThrow().toArray(new Object[] {}));
    log.info("Connection Manager Workflow registered.");
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated
  private void registerDiscover(final WorkerFactory factory) {
    final Set<String> taskQueues = getDiscoverTaskQueue();
    for (final String taskQueue : taskQueues) {
      final Worker discoverWorker = factory.newWorker(taskQueue, getWorkerOptions(1));
      final WorkflowImplementationOptions options = WorkflowImplementationOptions.newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();
      discoverWorker
          .registerWorkflowImplementationTypes(options,
              temporalProxyHelper.proxyWorkflowClass(DiscoverCatalogWorkflowImpl.class),
              temporalProxyHelper.proxyWorkflowClass(DiscoverCatalogAndAutoPropagateWorkflowImpl.class));
      log.info("Discover Workflow registered.");
    }
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated
  private void registerGetSpec(final WorkerFactory factory) {
    final Worker specWorker = factory.newWorker(TemporalJobType.GET_SPEC.name(), getWorkerOptions(1));
    final WorkflowImplementationOptions options = WorkflowImplementationOptions.newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();
    specWorker.registerWorkflowImplementationTypes(options,
        temporalProxyHelper.proxyWorkflowClass(SpecWorkflowImpl.class));
    log.info("Get Spec Workflow registered.");
  }

  private void registerSync(final WorkerFactory factory, final MaxWorkersConfig maxWorkersConfig) {
    final Set<String> taskQueues = getSyncTaskQueue();

    // There should be a default value provided by the application framework. If not, do this
    // as a safety check to ensure we don't attempt to register against no task queue.
    if (taskQueues.isEmpty()) {
      throw new IllegalStateException("Sync workflow task queue must be provided.");
    }

    for (final String taskQueue : taskQueues) {
      log.info("Registering sync workflow for task queue '{}'...", taskQueue);
      final Worker syncWorker = factory.newWorker(taskQueue,
          getWorkerOptions(maxWorkersConfig.getMaxSyncWorkers()));
      final WorkflowImplementationOptions options = WorkflowImplementationOptions.newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException.class).build();
      syncWorker.registerWorkflowImplementationTypes(options,
          temporalProxyHelper.proxyWorkflowClass(SyncWorkflowImpl.class));
      syncWorker.registerActivitiesImplementations(
          syncActivities.orElseThrow().toArray(new Object[] {}));

      log.info("Registering connector command workflow for task queue '{}'...", taskQueue);
      syncWorker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(ConnectorCommandWorkflowImpl.class));
      syncWorker.registerActivitiesImplementations(uiCommandsActivities.orElseThrow().toArray(new Object[] {}));
    }
    log.info("Sync Workflow registered.");
  }

  private WorkerOptions getWorkerOptions(final int max) {
    return WorkerOptions.newBuilder()
        .setMaxConcurrentActivityExecutionSize(max)
        .setMaxConcurrentWorkflowTaskExecutionSize(inferWorkflowExecSizeFromActivityExecutionSize(max))
        .build();
  }

  @VisibleForTesting
  static int inferWorkflowExecSizeFromActivityExecutionSize(final int max) {
    // Divide by 5 seems to be a good ratio given current empirical observations
    // Keeping floor at 2 to ensure we keep always return a valid value
    final int floor = 2;
    final int maxWorkflowSize = max / 5;
    return Math.max(maxWorkflowSize, floor);
  }

  /**
   * Performs additional configuration of the Temporal service/connection.
   *
   * @param temporalUtils A {@link TemporalUtils} instance.
   * @param temporalService A {@link WorkflowServiceStubs} instance.
   * @throws ExecutionException if unable to perform the additional configuration.
   * @throws InterruptedException if unable to perform the additional configuration.
   * @throws TimeoutException if unable to perform the additional configuration.
   */
  private void configureTemporal(final TemporalUtils temporalUtils,
                                 final WorkflowServiceStubs temporalService)
      throws ExecutionException, InterruptedException, TimeoutException {
    log.info("Configuring Temporal....");
    // Create the default Temporal namespace
    temporalUtils.configureTemporalNamespace(temporalService);

    // Ensure that the Temporal namespace exists before continuing.
    // If it does not exist after 30 seconds, fail the startup.
    executorService.submit(temporalInitializationUtils::waitForTemporalNamespace)
        .get(30, TimeUnit.SECONDS);
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private Set<String> getSyncTaskQueue() {
    if (StringUtils.isEmpty(syncTaskQueue)) {
      return Set.of();
    }
    return Arrays.stream(syncTaskQueue.split(",")).collect(Collectors.toSet());
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private Set<String> getCheckTaskQueue() {
    if (StringUtils.isEmpty(checkTaskQueue)) {
      return Set.of();
    }
    return Arrays.stream(checkTaskQueue.split(",")).collect(Collectors.toSet());
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private Set<String> getDiscoverTaskQueue() {
    if (StringUtils.isEmpty(discoverTaskQueue)) {
      return Set.of();
    }
    return Arrays.stream(discoverTaskQueue.split(",")).collect(Collectors.toSet());
  }

}
