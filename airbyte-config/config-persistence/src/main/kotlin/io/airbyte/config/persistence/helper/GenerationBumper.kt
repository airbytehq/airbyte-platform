/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.helper

import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamGeneration
import io.airbyte.config.persistence.domain.StreamRefresh
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class GenerationBumper(
  private val streamGenerationRepository: StreamGenerationRepository,
) {
  /**
   * This is increasing the generation of the stream being refreshed.
   * For each stream being refreshed, it fetches the current generation and then create a new entry in the generation
   * table with the generation being bumped.
   * @param connectionId - the connectionId of the generation being increased
   * @param jobId - The current jobId
   * @param streamRefreshes - List of the stream being refreshed
   */
  fun updateGenerationForStreams(
    connectionId: UUID,
    jobId: Long,
    streamRefreshes: List<StreamRefresh>,
    fullRefreshStream: Set<StreamDescriptor>,
  ) {
    val streamDescriptors: Set<StreamDescriptor> =
      streamRefreshes
        .map { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) }
        .toHashSet() + fullRefreshStream

    val currentMaxGeneration: List<Generation> = streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId)

    val streamDescriptorWithoutAGeneration =
      streamDescriptors.filter {
        val missingInGeneration =
          currentMaxGeneration.find { generation: Generation ->
            generation.streamName == it.name && generation.streamNamespace == it.namespace
          } == null
        missingInGeneration
      }

    val newGenerations =
      streamDescriptorWithoutAGeneration.map {
        Generation(
          streamName = it.name,
          streamNamespace = it.namespace,
          generationId = 0L,
        )
      }

    val generationToUpdate: List<Generation> =
      currentMaxGeneration.filter {
        val streamDescriptor = StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace)
        streamDescriptors.contains(streamDescriptor)
      } + newGenerations

    val updatedStreamGeneration =
      generationToUpdate.map {
        StreamGeneration(
          id = UUID.randomUUID(),
          connectionId = connectionId,
          streamName = it.streamName,
          streamNamespace = it.streamNamespace,
          generationId = it.generationId + 1,
          startJobId = jobId,
        )
      }

    streamGenerationRepository.saveAll(updatedStreamGeneration)
  }
}
