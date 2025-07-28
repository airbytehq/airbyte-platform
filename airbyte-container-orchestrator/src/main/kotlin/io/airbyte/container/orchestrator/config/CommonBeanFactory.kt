/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.container.orchestrator.worker.BufferConfiguration
import io.airbyte.container.orchestrator.worker.DestinationStarter
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.ReplicationWorkerContext
import io.airbyte.container.orchestrator.worker.SourceStarter
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.container.orchestrator.worker.withBufferSize
import io.airbyte.container.orchestrator.worker.withDefaultConfiguration
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.pod.FileConstants
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.annotation.Nullable
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.function.Supplier
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Defines and creates any singletons that are only required when running in any mode.
 */
@Factory
class CommonBeanFactory {
  @Singleton
  @Named("attemptId")
  fun attemptId(jobRunConfig: JobRunConfig) = Math.toIntExact(jobRunConfig.attemptId)

  @Singleton
  @Named("connectionId")
  fun connectionId(replicationInput: ReplicationInput): UUID = replicationInput.connectionId

  /**
   * Returns the config directory which contains all the configuration files.
   *
   * @param configDir optional directory, defaults to FileConstants.CONFIG_DIR if not defined.
   * @return Configuration directory.
   */
  @Singleton
  @Named("configDir")
  fun configDir(
    @Value("\${airbyte.config-dir}") @Nullable configDir: String?,
  ): String {
    if (configDir == null) {
      return FileConstants.CONFIG_DIR
    }
    return configDir
  }

  /**
   * Returns the contents of the OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG file.
   *
   * @param jobId Which job is being run.
   * @param attemptId Which attempt of the job is being run.
   * @return Contents of OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG
   */
  @Singleton
  fun jobRunConfig(
    @Value("\${airbyte.job-id}") @Nullable jobId: String,
    @Value("\${airbyte.attempt-id}") @Nullable attemptId: Long,
  ): JobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptId)

  @Singleton
  @Named("jobRoot")
  fun jobRoot(
    jobRunConfig: JobRunConfig,
    @Named("workspaceRoot") workspaceRoot: Path,
  ): Path =
    TemporalUtils.getJobRoot(
      workspaceRoot,
      jobRunConfig.jobId,
      jobRunConfig.attemptId,
    )

  @Singleton
  @Named("workspaceRoot")
  fun workspaceRoot(
    @Value("\${airbyte.workspace-root}") workspaceRoot: String,
  ): Path = Path.of(workspaceRoot)

  @Singleton
  @Named("workloadId")
  fun workloadId(
    @Value("\${airbyte.workload-id}") workloadId: String,
  ): String = workloadId

  @Singleton
  fun replicationInput(
    @Named("configDir") configDir: String,
  ): ReplicationInput =
    Jsons.deserialize(
      Path.of(configDir).resolve(FileConstants.INIT_INPUT_FILE).toFile(),
      ReplicationInput::class.java,
    )

  @Singleton
  @Named("workspaceId")
  fun workspaceId(replicationInput: ReplicationInput): UUID = replicationInput.workspaceId

  @Singleton
  @Named("stateDocumentStore")
  fun documentStoreClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.STATE)

  @Singleton
  @Named("epochMilliSupplier")
  fun epochMilliSupplier() = Supplier { Instant.now().toEpochMilli() }

  @Singleton
  @Named("idSupplier")
  fun idSupplier() = Supplier { UUID.randomUUID() }

  @Singleton
  @Named("outputDocumentStore")
  fun outputDocumentStoreClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  @Named("replicationWorkerDispatcher")
  fun replicationWorkerDispatcher(
    @Value("\${airbyte.replication.dispatcher.n-threads:4}") nThreads: Int,
  ): ExecutorService = Executors.newFixedThreadPool(nThreads)

  @Singleton
  @Named("syncPersistenceExecutorService")
  fun syncPersistenceExecutorService(): ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  @Singleton
  fun bufferConfiguration(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader): BufferConfiguration {
    val bufferSize = replicationInputFeatureFlagReader.read(ReplicationBufferOverride)
    return if (bufferSize > 0) withBufferSize(bufferSize) else withDefaultConfiguration()
  }

  @Singleton
  fun replicationContext(
    replicationContextProvider: ReplicationContextProvider,
    replicationInput: ReplicationInput,
  ) = replicationContextProvider.provideContext(replicationInput)

  @Singleton
  @Named("onReplicationRunning")
  fun replicationRunningCallback(
    @Named("workloadId") workloadId: String,
  ): VoidCallable =
    object : VoidCallable {
      override fun voidCall() {
        println("workloadId = $workloadId")
      }
    }

  @Singleton
  @Named("startReplicationJobs")
  fun startReplicationJobs(
    destination: AirbyteDestination,
    @Named("jobRoot") jobRoot: Path,
    replicationInput: ReplicationInput,
    replicationWorkerContext: ReplicationWorkerContext,
    source: AirbyteSource,
  ) = listOf(
    DestinationStarter(
      destination = destination,
      jobRoot = jobRoot,
      context = replicationWorkerContext,
    ),
    SourceStarter(
      source = source,
      jobRoot = jobRoot,
      replicationInput = replicationInput,
      context = replicationWorkerContext,
    ),
  )

  @Singleton
  @Named("sourceMessageQueue")
  fun sourceMessageQueue(context: ReplicationWorkerContext) = ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.sourceMaxBufferSize)

  @Singleton
  @Named("replicationMdcScopeBuilder")
  fun replicationMdcScopeBuilder(
    @Named("jobRoot") jobRoot: Path,
    logMdcHelper: LogMdcHelper,
  ): MdcScope.Builder =
    MdcScope
      .Builder()
      .setExtraMdcEntries(
        mapOf(
          LOG_SOURCE_MDC_KEY to LogSource.REPLICATION_ORCHESTRATOR.displayName,
          logMdcHelper.getJobLogPathMdcKey() to logMdcHelper.fullLogPath(jobRoot),
        ),
      )

  @Singleton
  @Named("workloadHeartbeatInterval")
  fun workloadHeartbeatInterval(
    @Value("\${airbyte.workload-api.heartbeat.interval-seconds}") interval: Long,
  ): Duration = interval.seconds.toJavaDuration()

  @Singleton
  @Named("workloadHeartbeatTimeout")
  fun workloadHeartbeatTimeout(
    @Value("\${airbyte.workload-api.heartbeat.timeout-seconds}") timeout: Long,
  ): Duration = timeout.seconds.toJavaDuration()

  @Singleton
  @Named("hardExitCallable")
  fun hardExitCallable(): () -> Unit = { kotlin.system.exitProcess(2) }
}
