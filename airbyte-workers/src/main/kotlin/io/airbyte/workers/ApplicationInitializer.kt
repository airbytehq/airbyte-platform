/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import datadog.trace.api.GlobalTracer
import io.airbyte.commons.temporal.TemporalInitializationUtils
import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.config.TemporalQueueConfiguration
import io.airbyte.commons.temporal.scheduling.ActorDefinitionUpdateWorkflow
import io.airbyte.config.MaxWorkersConfig
import io.airbyte.micronaut.runtime.AirbyteDataPlaneQueueConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.workers.temporal.TemporalWorkerShutdownHook
import io.airbyte.workers.temporal.check.connection.CheckConnectionWorkflowImpl
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogWorkflowImpl
import io.airbyte.workers.temporal.jobpostprocessing.JobPostProcessingWorkflowImpl
import io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowImpl
import io.airbyte.workers.temporal.spec.SpecWorkflowImpl
import io.airbyte.workers.temporal.sync.SyncWorkflowImpl
import io.airbyte.workers.temporal.sync.SyncWorkflowV2Impl
import io.airbyte.workers.temporal.workflows.ActorDefinitionUpdateWorkflowImpl
import io.airbyte.workers.temporal.workflows.ConnectorCommandWorkflowImpl
import io.airbyte.workers.temporal.workflows.DiscoverCatalogAndAutoPropagateWorkflowImpl
import io.airbyte.workers.tracing.StorageObjectGetInterceptor
import io.airbyte.workers.tracing.TemporalSdkInterceptor
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.core.util.StringUtils
import io.micronaut.discovery.event.ServiceReadyEvent
import io.micronaut.scheduling.TaskExecutors
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.NonDeterministicException
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerOptions
import io.temporal.worker.WorkflowImplementationOptions
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.Optional
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.max

/**
 * Performs any required initialization logic on application context start.
 */
@Singleton
@Requires(notEnv = [Environment.TEST])
class ApplicationInitializer(
  @param:Named("uiCommandsActivities") private val uiCommandsActivities: Optional<MutableList<Any>>,
  @param:Named("connectionManagerActivities") private val connectionManagerActivities: Optional<MutableList<Any>>,
  @param:Named("jobPostProcessingActivities") private val jobPostProcessingActivities: Optional<MutableList<Any>>,
  @param:Named(TaskExecutors.IO) private val executorService: ExecutorService,
  @param:Named("syncActivities") private val syncActivities: Optional<MutableList<Any>>,
  private val temporalInitializationUtils: TemporalInitializationUtils,
  private val temporalProxyHelper: TemporalProxyHelper,
  private val temporalService: WorkflowServiceStubs,
  private val temporalUtils: TemporalUtils,
  private val workerFactory: WorkerFactory,
  private val temporalQueueConfiguration: TemporalQueueConfiguration,
  private val airbyteDataPlaneQueueConfig: AirbyteDataPlaneQueueConfig,
  private val airbyteWorkerConfig: AirbyteWorkerConfig,
) : ApplicationEventListener<ServiceReadyEvent?> {
  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    try {
      configureTracer()
      initializeCommonDependencies()

      registerWorkerFactory(
        workerFactory,
        MaxWorkersConfig(
          airbyteWorkerConfig.check.maxWorkers,
          airbyteWorkerConfig.discover.maxWorkers,
          airbyteWorkerConfig.spec.maxWorkers,
          airbyteWorkerConfig.sync.maxWorkers,
          airbyteWorkerConfig.notify.maxWorkers,
        ),
      )

      log.info("Starting worker factory...")
      workerFactory.start()

      log.info("Application initialized.")
    } catch (e: ExecutionException) {
      log.error("Unable to initialize application.", e)
      throw IllegalStateException(e)
    } catch (e: InterruptedException) {
      log.error("Unable to initialize application.", e)
      throw IllegalStateException(e)
    } catch (e: TimeoutException) {
      log.error("Unable to initialize application.", e)
      throw IllegalStateException(e)
    }
  }

  private fun configureTracer() {
    val globalTracer = GlobalTracer.get()
    globalTracer.addTraceInterceptor(StorageObjectGetInterceptor())
    globalTracer.addTraceInterceptor(TemporalSdkInterceptor())
  }

  private fun initializeCommonDependencies() {
    log.info("Initializing common worker dependencies.")

    configureTemporal(temporalUtils, temporalService)
  }

  private fun registerWorkerFactory(
    workerFactory: WorkerFactory,
    maxWorkersConfiguration: MaxWorkersConfig,
  ) {
    log.info("Registering worker factories....")
    registerUiCommandsWorker(workerFactory, maxWorkersConfiguration)

    if (airbyteWorkerConfig.sync.enabled) {
      registerSync(workerFactory, maxWorkersConfiguration)
    }

    if (airbyteWorkerConfig.connection.enabled) {
      registerConnectionManager(workerFactory, maxWorkersConfiguration)
    }

    if (airbyteWorkerConfig.spec.enabled) {
      registerGetSpec(workerFactory)
    }

    if (airbyteWorkerConfig.check.enabled) {
      registerCheckConnection(workerFactory)
    }

    if (airbyteWorkerConfig.discover.enabled) {
      registerDiscover(workerFactory)
    }
  }

  private fun registerUiCommandsWorker(
    factory: WorkerFactory,
    maxWorkersConfiguration: MaxWorkersConfig,
  ) {
    val uiCommandsWorker =
      factory.newWorker(temporalQueueConfiguration.uiCommandsQueue, getWorkerOptions(maxWorkersConfiguration.maxCheckWorkers))
    val workflowOptions =
      WorkflowImplementationOptions
        .newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
        .build()

    uiCommandsWorker.registerWorkflowImplementationTypes(
      workflowOptions,
      temporalProxyHelper.proxyWorkflowClass(ConnectorCommandWorkflowImpl::class.java),
      temporalProxyHelper.proxyWorkflowClass(ActorDefinitionUpdateWorkflowImpl::class.java),
    )
    uiCommandsWorker.registerActivitiesImplementations(*uiCommandsActivities.orElseThrow().toTypedArray())
    log.info("UI Commands Worker registered.")
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated("")
  private fun registerCheckConnection(factory: WorkerFactory) {
    val taskQueues = getCheckTaskQueue()
    for (taskQueue in taskQueues) {
      val checkConnectionWorker = factory.newWorker(taskQueue, getWorkerOptions(1))
      val options =
        WorkflowImplementationOptions
          .newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
          .build()
      checkConnectionWorker
        .registerWorkflowImplementationTypes(
          options,
          temporalProxyHelper.proxyWorkflowClass(CheckConnectionWorkflowImpl::class.java),
        )
      log.info("Check Connection Workflow registered.")
    }
  }

  private fun registerConnectionManager(
    factory: WorkerFactory,
    maxWorkersConfig: MaxWorkersConfig,
  ) {
    val connectionUpdaterWorker =
      factory.newWorker(
        TemporalJobType.CONNECTION_UPDATER.toString(),
        getWorkerOptions(maxWorkersConfig.maxSyncWorkers),
      )
    val options =
      WorkflowImplementationOptions
        .newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
        .build()
    connectionUpdaterWorker
      .registerWorkflowImplementationTypes(
        options,
        temporalProxyHelper.proxyWorkflowClass(ConnectionManagerWorkflowImpl::class.java),
        temporalProxyHelper.proxyWorkflowClass(JobPostProcessingWorkflowImpl::class.java),
      )
    connectionUpdaterWorker
      .registerActivitiesImplementations(*connectionManagerActivities.orElseThrow().toTypedArray())
    log.info("Connection Manager Workflow registered.")
    connectionUpdaterWorker
      .registerActivitiesImplementations(*jobPostProcessingActivities.orElseThrow().toTypedArray())
    log.info("Job Post Processing Workflow registered.")
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated("")
  private fun registerDiscover(factory: WorkerFactory) {
    val taskQueues = getDiscoverTaskQueue()
    for (taskQueue in taskQueues) {
      val discoverWorker = factory.newWorker(taskQueue, getWorkerOptions(1))
      val options =
        WorkflowImplementationOptions
          .newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
          .build()
      discoverWorker
        .registerWorkflowImplementationTypes(
          options,
          temporalProxyHelper.proxyWorkflowClass(DiscoverCatalogWorkflowImpl::class.java),
          temporalProxyHelper.proxyWorkflowClass(DiscoverCatalogAndAutoPropagateWorkflowImpl::class.java),
        )
      log.info("Discover Workflow registered.")
    }
  }

  // This workflow is now deprecated, it remains to provide an explicit failure in case of a migration
  @Deprecated("")
  private fun registerGetSpec(factory: WorkerFactory) {
    val specWorker = factory.newWorker(TemporalJobType.GET_SPEC.name, getWorkerOptions(1))
    val options =
      WorkflowImplementationOptions
        .newBuilder()
        .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
        .build()
    specWorker.registerWorkflowImplementationTypes(
      options,
      temporalProxyHelper.proxyWorkflowClass(SpecWorkflowImpl::class.java),
    )
    log.info("Get Spec Workflow registered.")
  }

  private fun registerSync(
    factory: WorkerFactory,
    maxWorkersConfig: MaxWorkersConfig,
  ) {
    val taskQueues = getSyncTaskQueue()

    // There should be a default value provided by the application framework. If not, do this
    // as a safety check to ensure we don't attempt to register against no task queue.
    check(!taskQueues.isEmpty()) { "Sync workflow task queue must be provided." }

    for (taskQueue in taskQueues) {
      log.info("Registering sync workflow for task queue '{}'...", taskQueue)
      val syncWorker =
        factory.newWorker(
          taskQueue,
          getWorkerOptions(maxWorkersConfig.maxSyncWorkers),
        )
      val options =
        WorkflowImplementationOptions
          .newBuilder()
          .setFailWorkflowExceptionTypes(NonDeterministicException::class.java)
          .build()
      syncWorker.registerWorkflowImplementationTypes(
        options,
        temporalProxyHelper.proxyWorkflowClass(SyncWorkflowImpl::class.java),
        temporalProxyHelper.proxyWorkflowClass(SyncWorkflowV2Impl::class.java),
      )
      syncWorker.registerActivitiesImplementations(
        *syncActivities.orElseThrow().toTypedArray(),
      )

      log.info("Registering connector command workflow for task queue '{}'...", taskQueue)
      syncWorker.registerWorkflowImplementationTypes(
        temporalProxyHelper.proxyWorkflowClass(
          ConnectorCommandWorkflowImpl::class.java,
        ),
      )
      syncWorker.registerActivitiesImplementations(*uiCommandsActivities.orElseThrow().toTypedArray())
    }
    log.info("Sync Workflow registered.")
  }

  private fun getWorkerOptions(max: Int): WorkerOptions =
    WorkerOptions
      .newBuilder()
      .setMaxConcurrentActivityExecutionSize(max)
      .setMaxConcurrentWorkflowTaskExecutionSize(max)
      .build()

  /**
   * Performs additional configuration of the Temporal service/connection.
   *
   * @param temporalUtils A [TemporalUtils] instance.
   * @param temporalService A [WorkflowServiceStubs] instance.
   * @throws ExecutionException if unable to perform the additional configuration.
   * @throws InterruptedException if unable to perform the additional configuration.
   * @throws TimeoutException if unable to perform the additional configuration.
   */
  private fun configureTemporal(
    temporalUtils: TemporalUtils,
    temporalService: WorkflowServiceStubs,
  ) {
    log.info("Configuring Temporal....")
    // Create the default Temporal namespace
    temporalUtils.configureTemporalNamespace(temporalService)

    // Ensure that the Temporal namespace exists before continuing.
    // If it does not exist after 30 seconds, fail the startup.
    executorService.submit { temporalInitializationUtils.waitForTemporalNamespace() }[30, TimeUnit.SECONDS]
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private fun getSyncTaskQueue(): Set<String> {
    if (StringUtils.isEmpty(airbyteDataPlaneQueueConfig.sync.taskQueue)) {
      return setOf()
    }
    return airbyteDataPlaneQueueConfig.sync.taskQueue
      .split(",")
      .filter { it.isNotEmpty() }
      .toSet()
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private fun getCheckTaskQueue(): Set<String> {
    if (StringUtils.isEmpty(airbyteDataPlaneQueueConfig.check.taskQueue)) {
      return setOf()
    }
    return airbyteDataPlaneQueueConfig.check.taskQueue
      .split(",")
      .filter { it.isNotEmpty() }
      .toSet()
  }

  /**
   * Retrieve and parse the sync workflow task queue configuration.
   *
   * @return A set of Temporal task queues for the sync workflow.
   */
  private fun getDiscoverTaskQueue(): Set<String> {
    if (StringUtils.isEmpty(airbyteDataPlaneQueueConfig.discover.taskQueue)) {
      return setOf()
    }
    return airbyteDataPlaneQueueConfig.discover.taskQueue
      .split(",")
      .filter { it.isNotEmpty() }
      .toSet()
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
