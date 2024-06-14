package io.airbyte.data.services.impls.data

import io.airbyte.config.StreamDescriptor
import io.airbyte.data.repositories.specialized.LastJobWithStatsPerStreamRepository
import io.airbyte.data.services.StreamStatsService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamStatsServiceDataImpl(
  private val lastJobWithStatsPerStreamRepository: LastJobWithStatsPerStreamRepository,
) : StreamStatsService {
  override fun getLastJobIdWithStatsByStream(
    connectionId: UUID,
    streams: Collection<StreamDescriptor>,
  ): Map<StreamDescriptor, Long> {
    val streamDescriptorList = streams.toList()
    val streamNames = streamDescriptorList.map { it.name }.toTypedArray()
    val streamNamespaces = streamDescriptorList.map { it.namespace }.toTypedArray()

    val result = lastJobWithStatsPerStreamRepository.findLastJobIdWithStatsPerStream(connectionId, streamNames, streamNamespaces)
    return result.associate { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) to it.jobId }
  }
}
