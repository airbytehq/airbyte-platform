package io.airbyte.config.persistence.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.config.RefreshStream
import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.DestinationSyncMode
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.protocol.models.SyncMode
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

    catalogCopy.streams.forEach {
        configuredAirbyteStream ->
      val streamDescriptor =
        StreamDescriptor().withName(
          configuredAirbyteStream.stream.name,
        ).withNamespace(configuredAirbyteStream.stream.namespace)
      val currentGeneration = generationByStreamDescriptor.getOrDefault(streamDescriptor, 0)
      val shouldTruncate: Boolean =
        refreshTypeByStream[streamDescriptor] == RefreshStream.RefreshType.TRUNCATE ||
          (configuredAirbyteStream.syncMode == SyncMode.FULL_REFRESH && configuredAirbyteStream.destinationSyncMode == DestinationSyncMode.OVERWRITE)

      configuredAirbyteStream.syncId = jobId
      configuredAirbyteStream.generationId = currentGeneration
      configuredAirbyteStream.minimumGenerationId = if (shouldTruncate) currentGeneration else 0
    }

    return catalogCopy
  }

  private fun getCurrentGenerationByStreamDescriptor(generations: List<Generation>): Map<StreamDescriptor, Long> {
    return generations
      .map { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) to it.generationId }.toMap()
  }

  private fun getStreamRefreshesAsStreamDescriptors(streamRefreshes: List<StreamRefresh>): Set<StreamDescriptor> {
    return streamRefreshes.map {
      StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace)
    }.toHashSet()
  }
}
