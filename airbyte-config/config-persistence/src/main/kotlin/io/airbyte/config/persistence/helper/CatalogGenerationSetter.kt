package io.airbyte.config.persistence.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.StreamDescriptor
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class CatalogGenerationSetter(private val streamGenerationRepository: StreamGenerationRepository) {
  fun updateCatalogWithGenerationAndSyncInformation(
    catalog: ConfiguredAirbyteCatalog,
    connectionId: UUID,
    jobId: Long,
    streamRefreshes: List<StreamRefresh>,
  ): ConfiguredAirbyteCatalog {
    val generationByStreamDescriptor: Map<StreamDescriptor, Long> = getCurrentGenerationByStreamDescriptor(connectionId)
    val uniqStreamRefresh: Set<StreamDescriptor> = getStreamRefreshesAsStreamDescriptors(streamRefreshes)

    val catalogCopy = Jsons.clone(catalog)

    catalogCopy.streams.forEach {
        configuredAirbyteStream ->
      val streamDescriptor =
        StreamDescriptor().withName(
          configuredAirbyteStream.stream.name,
        ).withNamespace(configuredAirbyteStream.stream.namespace)
      val currentVersion = generationByStreamDescriptor.getOrDefault(streamDescriptor, 0)
      val isRefresh = uniqStreamRefresh.contains(streamDescriptor)

      configuredAirbyteStream.syncId = jobId
      configuredAirbyteStream.generationId = currentVersion
      configuredAirbyteStream.minimumGenerationId = if (isRefresh) currentVersion else 0
    }

    return catalogCopy
  }

  private fun getCurrentGenerationByStreamDescriptor(connectionId: UUID): Map<StreamDescriptor, Long> {
    return streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId)
      .map { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) to it.generationId }.toMap()
  }

  private fun getStreamRefreshesAsStreamDescriptors(streamRefreshes: List<StreamRefresh>): Set<StreamDescriptor> {
    return streamRefreshes.map {
      StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace)
    }.toHashSet()
  }
}
