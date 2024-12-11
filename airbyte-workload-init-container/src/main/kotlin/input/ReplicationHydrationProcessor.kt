package io.airbyte.initContainer.input

import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Parses, hydrates and writes input files for the replication pod.
 */
@Requires(property = "airbyte.init.operation", pattern = "sync")
@Singleton
class ReplicationHydrationProcessor(
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val protocolSerializer: ProtocolSerializer,
  private val fileClient: FileClient,
  private val destinationCatalogGenerator: DestinationCatalogGenerator,
  private val metricClient: MetricClient,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    logger.info { "Deserializing replication input..." }
    val rawPayload = workload.inputPayload
    val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)

    logger.info { "Hydrating replication input..." }
    val hydrated: ReplicationInput = replicationInputHydrator.getHydratedReplicationInput(parsed)

    // orchestrator input
    logger.info { "Writing orchestrator inputs..." }
    fileClient.writeInputFile(
      FileConstants.INIT_INPUT_FILE,
      serializer.serialize(hydrated),
    )

    // source inputs
    logger.info { "Writing source inputs..." }
    fileClient.writeInputFile(
      FileConstants.CATALOG_FILE,
      protocolSerializer.serialize(hydrated.catalog, false),
      SOURCE_DIR,
    )

    fileClient.writeInputFile(
      FileConstants.CONNECTOR_CONFIG_FILE,
      serializer.serialize(hydrated.sourceConfiguration),
      SOURCE_DIR,
    )

    // no need to pass state if empty
    hydrated.state?.state?.let {
      fileClient.writeInputFile(
        FileConstants.INPUT_STATE_FILE,
        serializer.serialize(it),
        SOURCE_DIR,
      )
    }

    // dest inputs
    logger.info { "Writing destination inputs..." }
    val mapper =
      NamespacingMapper(
        hydrated.namespaceDefinition,
        hydrated.namespaceFormat,
        hydrated.prefix,
      )

    val transformedCatalog = destinationCatalogGenerator.generateDestinationCatalog(hydrated.catalog)

    parsed.connectionId?.let {
      sendMapperErrorMetrics(transformedCatalog, it)
    }

    val destinationCatalog = mapper.mapCatalog(destinationCatalogGenerator.generateDestinationCatalog(hydrated.catalog).catalog)

    fileClient.writeInputFile(
      FileConstants.CATALOG_FILE,
      protocolSerializer.serialize(destinationCatalog, hydrated.destinationSupportsRefreshes),
      DEST_DIR,
    )

    fileClient.writeInputFile(
      FileConstants.CONNECTOR_CONFIG_FILE,
      serializer.serialize(hydrated.destinationConfiguration),
      DEST_DIR,
    )

    // pipes for passing messages between all three
    logger.info { "Making named pipes..." }
    fileClient.makeNamedPipes()
  }

  private fun sendMapperErrorMetrics(
    transformedCatalog: DestinationCatalogGenerator.CatalogGenerationResult,
    connectionId: UUID,
  ) {
    transformedCatalog.errors.entries.forEach { streamErrors ->
      streamErrors.value.values.forEach { streamError ->
        metricClient.count(
          OssMetricsRegistry.MAPPER_ERROR,
          1,
          MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
          MetricAttribute(MetricTags.FAILURE_TYPE, streamError.type.name),
        )
      }
    }
  }
}
