/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.input

import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.commons.protocol.SerializationTarget
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.models.ArchitectureConstants
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.domain.Workload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
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
  @Value("\${airbyte.platform-mode}") private val platformMode: String,
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
      protocolSerializer.serialize(hydrated.catalog, false, SerializationTarget.SOURCE),
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

    if (platformMode == ArchitectureConstants.BOOKKEEPER) {
      // Write original catalog as is
      fileClient.writeInputFile(
        FileConstants.CATALOG_FILE,
        protocolSerializer.serialize(hydrated.catalog, hydrated.destinationSupportsRefreshes, SerializationTarget.DESTINATION),
        DEST_DIR,
      )

      // Write namespace mapping info details for destination to generate the final catalog on its own
      fileClient.writeInputFile(
        FileConstants.NAMESPACE_MAPPING_FILE,
        serializer.serialize(NamespaceInfo(hydrated.namespaceDefinition, hydrated.namespaceFormat, hydrated.prefix)),
        DEST_DIR,
      )
    } else {
      val transformedCatalog = destinationCatalogGenerator.generateDestinationCatalog(hydrated.catalog)

      parsed.connectionId?.let {
        sendMapperErrorMetrics(transformedCatalog, it)
      }

      val destinationCatalog = mapper.mapCatalog(transformedCatalog.catalog)

      fileClient.writeInputFile(
        FileConstants.CATALOG_FILE,
        protocolSerializer.serialize(destinationCatalog, hydrated.destinationSupportsRefreshes, SerializationTarget.DESTINATION),
        DEST_DIR,
      )
    }
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
          metric = OssMetricsRegistry.MAPPER_ERROR,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
              MetricAttribute(MetricTags.FAILURE_TYPE, streamError.type.name),
            ),
        )
      }
    }
  }
}

data class NamespaceInfo(
  val namespaceDefinitionType: NamespaceDefinitionType?,
  val namespaceFormat: String?,
  val streamPrefix: String?,
)
