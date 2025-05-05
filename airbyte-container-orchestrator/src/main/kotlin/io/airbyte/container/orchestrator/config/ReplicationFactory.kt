/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.featureflag.WorkloadHeartbeatRate
import io.airbyte.featureflag.WorkloadHeartbeatTimeout
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.airbyte.workers.general.BufferConfiguration
import io.airbyte.workers.general.BufferConfiguration.Companion.withBufferSize
import io.airbyte.workers.general.BufferConfiguration.Companion.withDefaultConfiguration
import io.airbyte.workers.general.buffered.worker.ReplicationContextProvider
import io.airbyte.workers.general.buffered.worker.ReplicationWorker
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerContext
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerHelper
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerState
import io.airbyte.workers.general.buffered.worker.WorkloadHeartbeatSender
import io.airbyte.workers.helper.StreamStatusCompletionTracker
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteMapper
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.AnalyticsMessageTracker
import io.airbyte.workers.internal.DestinationTimeoutMonitor
import io.airbyte.workers.internal.FieldSelector
import io.airbyte.workers.internal.HeartbeatMonitor
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.airbyte.workers.tracker.ThreadedTimeTracker
import io.airbyte.workload.api.client.WorkloadApiClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class ReplicationFactory {
  @Singleton
  fun bufferConfiguration(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader): BufferConfiguration {
    val bufferSize = replicationInputFeatureFlagReader.read(ReplicationBufferOverride)
    return if (bufferSize > 0) withBufferSize(bufferSize) else withDefaultConfiguration()
  }

  @Singleton
  fun namespaceMapper(replicationInput: ReplicationInput) =
    NamespacingMapper(
      replicationInput.namespaceDefinition,
      replicationInput.namespaceFormat,
      replicationInput.prefix,
    )

  @Singleton
  fun replicationContextProvider(
    @Named("attemptId") attempt: Int,
    @Value("\${airbyte.job-id}") jobId: Long,
  ) = ReplicationContextProvider(jobId, attempt)

  @Singleton
  fun replicationContext(
    replicationContextProvider: ReplicationContextProvider,
    replicationInput: ReplicationInput,
  ) = replicationContextProvider.provideContext(replicationInput)

  @Singleton
  @Named("onReplicationRunning")
  fun replicationRunningCallback(
    @Named("workloadId") workloadId: String,
  ): VoidCallable = VoidCallable { workloadId }

  @Singleton
  fun replicationWorkerHelper(
    analyticsMessageTracker: AnalyticsMessageTracker,
    destinationCatalogGenerator: DestinationCatalogGenerator,
    fieldSelector: FieldSelector,
    mapper: AirbyteMapper,
    messageTracker: AirbyteMessageTracker,
    msgEventPublisher: ReplicationAirbyteMessageEventPublishingHelper,
    recordMapper: RecordMapper,
    replicationContext: ReplicationContextProvider.Context,
    replicationWorkerState: ReplicationWorkerState,
    streamStatusCompletionTracker: StreamStatusCompletionTracker,
    streamStatusTracker: StreamStatusTracker,
    syncPersistence: SyncPersistence,
    timeTracker: ThreadedTimeTracker,
    metricClient: MetricClient,
  ): ReplicationWorkerHelper =
    ReplicationWorkerHelper(
      analyticsTracker = analyticsMessageTracker,
      context = replicationContext,
      destinationCatalogGenerator = destinationCatalogGenerator,
      eventPublisher = msgEventPublisher,
      fieldSelector = fieldSelector,
      mapper = mapper,
      messageTracker = messageTracker,
      recordMapper = recordMapper,
      replicationWorkerState = replicationWorkerState,
      streamStatusCompletionTracker = streamStatusCompletionTracker,
      streamStatusTracker = streamStatusTracker,
      syncPersistence = syncPersistence,
      timeTracker = timeTracker,
      metricClient = metricClient,
    )

  @Singleton
  fun replicationWorkerState() = ReplicationWorkerState()

  @Singleton
  fun replicationWorkerContext(
    @Named("attemptId") attemptId: Int,
    bufferConfiguration: BufferConfiguration,
    @Value("\${airbyte.job-id}") jobId: Long,
    replicationWorkerHelper: ReplicationWorkerHelper,
    replicationWorkerState: ReplicationWorkerState,
    streamStatusCompletionTracker: StreamStatusCompletionTracker,
  ) = ReplicationWorkerContext(
    attempt = attemptId,
    bufferConfiguration = bufferConfiguration,
    jobId = jobId,
    replicationWorkerHelper = replicationWorkerHelper,
    replicationWorkerState = replicationWorkerState,
    streamStatusCompletionTracker = streamStatusCompletionTracker,
  )

  @Singleton
  fun workloadHeartbeatSender(
    @Named("attemptId") attemptId: Int,
    destinationTimeoutMonitor: DestinationTimeoutMonitor,
    @Value("\${airbyte.job-id}") jobId: Long,
    sourceHeartbeatMonitor: HeartbeatMonitor,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
    replicationWorkerState: ReplicationWorkerState,
    workloadApiClient: WorkloadApiClient,
    @Named("workloadId") workloadId: String,
  ) = WorkloadHeartbeatSender(
    attempt = attemptId,
    destinationTimeoutMonitor = destinationTimeoutMonitor,
    heartbeatInterval = Duration.ofSeconds(replicationInputFeatureFlagReader.read(WorkloadHeartbeatRate).toLong()),
    heartbeatTimeoutDuration = Duration.ofMinutes(replicationInputFeatureFlagReader.read(WorkloadHeartbeatTimeout).toLong()),
    jobId = jobId,
    replicationWorkerState = replicationWorkerState,
    sourceTimeoutMonitor = sourceHeartbeatMonitor,
    workloadApiClient = workloadApiClient,
    workloadId = workloadId,
  )

  @Singleton
  fun replicationWorker(
    destination: AirbyteDestination,
    @Named("onReplicationRunning") onReplicationRunning: VoidCallable,
    recordSchemaValidator: RecordSchemaValidator,
    replicationWorkerContext: ReplicationWorkerContext,
    source: AirbyteSource,
    syncPersistence: SyncPersistence,
    workloadHeartbeatSender: WorkloadHeartbeatSender,
  ) = ReplicationWorker(
    context = replicationWorkerContext,
    destination = destination,
    onReplicationRunning = onReplicationRunning,
    recordSchemaValidator = recordSchemaValidator,
    source = source,
    syncPersistence = syncPersistence,
    workloadHeartbeatSender = workloadHeartbeatSender,
  )
}
