/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.AttemptsRepository
import io.airbyte.data.repositories.StreamAttemptMetadataRepository
import jakarta.inject.Singleton

data class StreamAttemptMetadata(
  val streamName: String,
  val streamNamespace: String? = null,
  val wasBackfilled: Boolean,
  val wasResumed: Boolean,
)

@Singleton
class StreamAttemptMetadataService(
  private val attemptsRepository: AttemptsRepository,
  private val streamAttemptMetadataRepository: StreamAttemptMetadataRepository,
) {
  fun upsertStreamAttemptMetadata(
    jobId: Long,
    attemptNumber: Long,
    streamMetadata: List<StreamAttemptMetadata>,
  ) {
    val attemptId: Long = getAttemptId(jobId, attemptNumber)
    val entitiesToSave =
      streamMetadata.map {
        io.airbyte.data.repositories.entities.StreamAttemptMetadata(
          attemptId = attemptId,
          streamName = it.streamName,
          streamNamespace = it.streamNamespace,
          wasBackfilled = it.wasBackfilled,
          wasResumed = it.wasResumed,
        )
      }
    try {
      // Optimistic insertion here
      // We expect the default case to be always inserting new metadata and never to be updated,
      // but this can happen in case of retries.
      // The goal here is to simulate a `ON CONFLICT UPDATE` if we were to write SQL directly.
      streamAttemptMetadataRepository.saveAll(entitiesToSave)
    } catch (e: Exception) {
      val existingStreams =
        streamAttemptMetadataRepository
          .findAllByAttemptId(attemptId)
          .associate { Pair(it.streamName, it.streamNamespace) to it.id }
      val partitionedEntities =
        entitiesToSave
          .map { it.copy(id = existingStreams[Pair(it.streamName, it.streamNamespace)]) }
          .partition { it.id != null }
      streamAttemptMetadataRepository.saveAll(partitionedEntities.second)
      streamAttemptMetadataRepository.updateAll(partitionedEntities.first)
    }
  }

  fun getStreamAttemptMetadata(
    jobId: Long,
    attemptNumber: Long,
  ): List<StreamAttemptMetadata> {
    val attemptId: Long = getAttemptId(jobId, attemptNumber)
    val entities = streamAttemptMetadataRepository.findAllByAttemptId(attemptId)
    return entities.map {
      StreamAttemptMetadata(
        streamName = it.streamName,
        streamNamespace = it.streamNamespace,
        wasBackfilled = it.wasBackfilled,
        wasResumed = it.wasResumed,
      )
    }
  }

  private fun getAttemptId(
    jobId: Long,
    attemptNumber: Long,
  ): Long =
    attemptsRepository.findByJobIdAndAttemptNumber(jobId, attemptNumber)?.id
      ?: throw NoSuchElementException("No attempt found for jobId:$jobId and attemptNumber:$attemptNumber")
}
