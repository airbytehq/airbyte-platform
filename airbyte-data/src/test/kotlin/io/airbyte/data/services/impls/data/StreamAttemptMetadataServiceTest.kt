/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.repositories.AttemptsRepository
import io.airbyte.data.repositories.StreamAttemptMetadataRepository
import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.data.services.StreamAttemptMetadata
import io.airbyte.data.services.StreamAttemptMetadataService
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@MicronautTest
internal class StreamAttemptMetadataServiceTest : AbstractConfigRepositoryTest() {
  val streamAttemptMetadataService = context.getBean(StreamAttemptMetadataService::class.java)!!

  @BeforeEach
  fun setupEach() {
    context.getBean(AttemptsRepository::class.java).deleteAll()
    context.getBean(StreamAttemptMetadataRepository::class.java).deleteAll()
  }

  @Test
  fun `test insertion and find by attempt id`() {
    val jobId = 12L
    createAttempt(jobId, 0, 12)
    createAttempt(jobId, 1, 13)

    val streamAttemptMetadata0 =
      listOf(
        StreamAttemptMetadata(streamName = "stream1", wasBackfilled = false, wasResumed = false),
        StreamAttemptMetadata(streamName = "stream2", streamNamespace = "ns1", wasBackfilled = false, wasResumed = true),
      )
    val streamAttemptMetadata1 =
      listOf(
        StreamAttemptMetadata(streamName = "s1", streamNamespace = "ns", wasBackfilled = true, wasResumed = true),
        StreamAttemptMetadata(streamName = "stream2", streamNamespace = "ns1", wasBackfilled = true, wasResumed = false),
      )
    streamAttemptMetadataService.upsertStreamAttemptMetadata(jobId, 0, streamAttemptMetadata0)
    streamAttemptMetadataService.upsertStreamAttemptMetadata(jobId, 1, streamAttemptMetadata1)

    val actualMetadata0 = streamAttemptMetadataService.getStreamAttemptMetadata(jobId, 0)
    assertThat(actualMetadata0).isEqualTo(streamAttemptMetadata0)

    val actualMetadata1 = streamAttemptMetadataService.getStreamAttemptMetadata(jobId, 1)
    assertThat(actualMetadata1).isEqualTo(streamAttemptMetadata1)
  }

  @Test
  fun `test upsert updates existing rows while adding new ones`() {
    val jobId = 13L
    createAttempt(jobId, 0, 10)
    createAttempt(jobId, 1, 11)

    val sanityCheck =
      listOf(
        StreamAttemptMetadata(streamName = "s1", wasBackfilled = true, wasResumed = true),
        StreamAttemptMetadata(streamName = "s1", streamNamespace = "ns1", wasBackfilled = true, wasResumed = true),
      )
    streamAttemptMetadataService.upsertStreamAttemptMetadata(jobId, 1, sanityCheck)

    val metadata0 =
      listOf(
        StreamAttemptMetadata(streamName = "s1", wasBackfilled = false, wasResumed = false),
        StreamAttemptMetadata(streamName = "s2", streamNamespace = "ns1", wasBackfilled = false, wasResumed = true),
      )
    streamAttemptMetadataService.upsertStreamAttemptMetadata(jobId, 0, metadata0)

    val actualMetadata0 = streamAttemptMetadataService.getStreamAttemptMetadata(jobId, 0)
    assertThat(actualMetadata0).isEqualTo(metadata0)

    val metadata1 =
      listOf(
        StreamAttemptMetadata(streamName = "s1", wasBackfilled = true, wasResumed = false),
        StreamAttemptMetadata(streamName = "s2", streamNamespace = "ns1", wasBackfilled = true, wasResumed = true),
        StreamAttemptMetadata(streamName = "s3", wasBackfilled = false, wasResumed = false),
      )
    streamAttemptMetadataService.upsertStreamAttemptMetadata(jobId, 0, metadata1)
    val actualMetadata1 = streamAttemptMetadataService.getStreamAttemptMetadata(jobId, 0)
    assertThat(actualMetadata1).isEqualTo(metadata1)

    // This is verifying that the upsert didn't modify extra rows from other attempts
    val actualSanityCheck = streamAttemptMetadataService.getStreamAttemptMetadata(jobId, 1)
    assertThat(actualSanityCheck).isEqualTo(sanityCheck)
  }

  private fun createAttempt(
    jobId: Long,
    attemptNumber: Long,
    attemptId: Long,
  ): Long =
    attemptsRepository
      .save(
        Attempt(id = attemptId, jobId = jobId, attemptNumber = attemptNumber),
      ).id ?: throw Exception("failed to create attempt for jobId:$jobId with attemptNumber:$attemptNumber")
}
