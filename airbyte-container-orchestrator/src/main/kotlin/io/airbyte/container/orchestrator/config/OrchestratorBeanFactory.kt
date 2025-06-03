/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.worker.DestinationReader
import io.airbyte.container.orchestrator.worker.DestinationWriter
import io.airbyte.container.orchestrator.worker.MessageProcessor
import io.airbyte.container.orchestrator.worker.RecordSchemaValidator
import io.airbyte.container.orchestrator.worker.ReplicationWorkerContext
import io.airbyte.container.orchestrator.worker.ReplicationWorkerHelper
import io.airbyte.container.orchestrator.worker.ReplicationWorkerState
import io.airbyte.container.orchestrator.worker.SourceReader
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.filter.FieldSelector
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.container.orchestrator.worker.util.ReplicationMetricReporter
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.models.ArchitectureConstants.ORCHESTRATOR
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Defines and creates any singletons that are only required when running in [ORCHESTRATOR] (legacy) mode.
 * <p />
 * Any singletons defined/created in this factory will only be available if the [PLATFORM_MODE]
 * environment variable contains the value [ORCHESTRATOR].
 */
@Factory
@Requires(property = PLATFORM_MODE, value = ORCHESTRATOR, defaultValue = ORCHESTRATOR)
class OrchestratorBeanFactory {
  @Singleton
  fun fieldSelector(
    recordSchemaValidator: RecordSchemaValidator,
    metricReporter: ReplicationMetricReporter,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  ) = FieldSelector(
    recordSchemaValidator = recordSchemaValidator,
    metricReporter = metricReporter,
    replicationInput = replicationInput,
    replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
  )

  @Singleton
  fun namespaceMapper(replicationInput: ReplicationInput) =
    NamespacingMapper(
      replicationInput.namespaceDefinition,
      replicationInput.namespaceFormat,
      replicationInput.prefix,
    )

  @Singleton
  @Named("streamNamesToSchemas")
  fun streamNamesToSchemas(replicationInput: ReplicationInput): MutableMap<AirbyteStreamNameNamespacePair, JsonNode?> =
    WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog)

  @Singleton
  @Named("schemaValidationExecutorService")
  fun schemaValidationExecutorService(): ExecutorService = Executors.newSingleThreadExecutor()

  @Singleton
  fun recordSchemaValidator(
    @Named("jsonSchemaValidator") jsonSchemaValidator: JsonSchemaValidator,
    @Named("schemaValidationExecutorService") schemaValidationExecutorService: ExecutorService,
    @Named("streamNamesToSchemas") streamNamesToSchemas: MutableMap<AirbyteStreamNameNamespacePair, JsonNode?>,
  ): RecordSchemaValidator =
    RecordSchemaValidator(
      jsonSchemaValidator = jsonSchemaValidator,
      schemaValidationExecutorService = schemaValidationExecutorService,
      streamNamesToSchemas = streamNamesToSchemas,
    )

  @Singleton
  @Named("destinationMessageQueue")
  fun destinationMessageQueue(context: ReplicationWorkerContext) =
    ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.destinationMaxBufferSize)

  @Singleton
  @Named("syncReplicationJobs")
  fun syncReplicationJobs(
    destination: AirbyteDestination,
    @Named("destinationMessageQueue") destinationMessageQueue: ClosableChannelQueue<AirbyteMessage>,
    replicationWorkerHelper: ReplicationWorkerHelper,
    replicationWorkerState: ReplicationWorkerState,
    source: AirbyteSource,
    @Named("sourceMessageQueue") sourceMessageQueue: ClosableChannelQueue<AirbyteMessage>,
    streamStatusCompletionTracker: StreamStatusCompletionTracker,
  ) = listOf(
    SourceReader(
      messagesFromSourceQueue = sourceMessageQueue,
      replicationWorkerState = replicationWorkerState,
      replicationWorkerHelper = replicationWorkerHelper,
      source = source,
      streamStatusCompletionTracker = streamStatusCompletionTracker,
    ),
    MessageProcessor(
      destinationQueue = destinationMessageQueue,
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
      sourceQueue = sourceMessageQueue,
    ),
    DestinationWriter(
      source = source,
      destination = destination,
      replicationWorkerState = replicationWorkerState,
      replicationWorkerHelper = replicationWorkerHelper,
      destinationQueue = destinationMessageQueue,
    ),
    DestinationReader(
      destination = destination,
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
    ),
  )
}
