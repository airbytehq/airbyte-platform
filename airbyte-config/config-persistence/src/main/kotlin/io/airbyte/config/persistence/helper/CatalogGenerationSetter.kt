/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.persistence.domain.Generation
import jakarta.inject.Singleton

@Singleton
class CatalogGenerationSetter {
  fun updateCatalogWithGenerationAndSyncInformation(
    catalog: ConfiguredAirbyteCatalog,
    jobId: Long,
    streamRefreshes: List<RefreshStream>,
    generations: List<Generation>,
  ): ConfiguredAirbyteCatalog {
    val generationByStreamDescriptor: Map<StreamDescriptor, Long> = getCurrentGenerationByStreamDescriptor(generations)

    val catalogCopy = Jsons.clone(catalog)
    val refreshTypeByStream = streamRefreshes.associate { it.streamDescriptor to it.refreshType }

    catalogCopy.streams.forEach { configuredAirbyteStream ->
      val streamDescriptor =
        StreamDescriptor()
          .withName(
            configuredAirbyteStream.stream.name,
          ).withNamespace(configuredAirbyteStream.stream.namespace)
      val currentGeneration = generationByStreamDescriptor.getOrDefault(streamDescriptor, 0)
      val shouldTruncate: Boolean =
        refreshTypeByStream[streamDescriptor] == RefreshStream.RefreshType.TRUNCATE ||
          (
            configuredAirbyteStream.syncMode == SyncMode.FULL_REFRESH &&
              (
                configuredAirbyteStream.destinationSyncMode == DestinationSyncMode.OVERWRITE ||
                  configuredAirbyteStream.destinationSyncMode == DestinationSyncMode.OVERWRITE_DEDUP
              )
          )

      setGenerationInformation(configuredAirbyteStream, jobId, currentGeneration, if (shouldTruncate) currentGeneration else 0)
    }

    return catalogCopy
  }

  fun updateCatalogWithGenerationAndSyncInformationForClear(
    catalog: ConfiguredAirbyteCatalog,
    jobId: Long,
    clearedStream: Set<StreamDescriptor>,
    generations: List<Generation>,
  ): ConfiguredAirbyteCatalog {
    val generationByStreamDescriptor: Map<StreamDescriptor, Long> = getCurrentGenerationByStreamDescriptor(generations)

    val catalogCopy = Jsons.clone(catalog)

    catalogCopy.streams.forEach { configuredAirbyteStream ->
      val streamDescriptor =
        StreamDescriptor()
          .withName(
            configuredAirbyteStream.stream.name,
          ).withNamespace(configuredAirbyteStream.stream.namespace)
      val currentGeneration = generationByStreamDescriptor.getOrDefault(streamDescriptor, 0)

      if (clearedStream.contains(streamDescriptor)) {
        setGenerationInformation(configuredAirbyteStream, jobId, currentGeneration, currentGeneration)
      } else {
        setGenerationInformation(configuredAirbyteStream, jobId, currentGeneration, 0)
      }
    }

    return catalogCopy
  }

  private fun setGenerationInformation(
    configuredAirbyteStream: ConfiguredAirbyteStream,
    jobId: Long,
    currentGenerationId: Long,
    minimumGenerationId: Long,
  ) {
    configuredAirbyteStream.syncId = jobId
    configuredAirbyteStream.generationId = currentGenerationId
    configuredAirbyteStream.minimumGenerationId = minimumGenerationId
  }

  private fun getCurrentGenerationByStreamDescriptor(generations: List<Generation>): Map<StreamDescriptor, Long> =
    generations.associate {
      StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) to it.generationId
    }
}
