/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

class WorkloadIdGeneratorTest {
  private val generator = WorkloadIdGenerator()

  @ParameterizedTest
  @MethodSource("workloadIdArgsMatrix")
  internal fun `test that the correct workload ID is generated for check`(
    actorId: UUID,
    jobId: String,
    attemptNumber: Int,
  ) {
    val generatedWorkloadId = generator.generateCheckWorkloadId(actorId, jobId, attemptNumber)
    Assertions.assertEquals(
      "${actorId}_${jobId}_${attemptNumber}_check",
      generatedWorkloadId,
    )
  }

  @ParameterizedTest
  @MethodSource("workloadIdArgsMatrix")
  internal fun `test that the correct workload ID is generated for discover`(
    actorId: UUID,
    jobId: String,
    attemptNumber: Int,
  ) {
    val generatedWorkloadId = generator.generateDiscoverWorkloadId(actorId, jobId, attemptNumber)
    Assertions.assertEquals(
      "${actorId}_${jobId}_${attemptNumber}_discover",
      generatedWorkloadId,
    )
  }

  @ParameterizedTest
  @MethodSource("actorTimestampMatrix")
  internal fun `the correct v2 workload ID is generated for discover`(
    actorId: UUID,
    timestampMs: Long,
  ) {
    val generatedWorkloadId = generator.generateDiscoverWorkloadIdV2(actorId, timestampMs)
    Assertions.assertEquals(
      "${actorId}_${timestampMs}_discover",
      generatedWorkloadId,
    )
  }

  @ParameterizedTest
  @MethodSource("windowSnapMatrix")
  internal fun `the correct v2 workload ID is generated for discover with snapping behavior`(
    timestampMs: Long,
    expectedSnappedTimestampMs: Long,
    windowWidthMs: Long,
  ) {
    val actorId = UUID.randomUUID()
    val generatedWorkloadId = generator.generateDiscoverWorkloadIdV2WithSnap(actorId, timestampMs, windowWidthMs)
    Assertions.assertEquals(
      "${actorId}_${expectedSnappedTimestampMs}_discover",
      generatedWorkloadId,
    )
  }

  @Test
  internal fun `test that the correct workload ID is generated for specs`() {
    val jobId = UUID.randomUUID()

    val generatedWorkloadId = generator.generateSpecWorkloadId(jobId.toString())
    Assertions.assertEquals(
      "${jobId}_spec",
      generatedWorkloadId,
    )
  }

  @Test
  internal fun `test that the correct workload ID is generated for syncs`() {
    val connectionId = UUID.randomUUID()
    val jobId = 12345L
    val attemptNumber = 1

    val generatedWorkloadId = generator.generateSyncWorkloadId(connectionId, jobId, attemptNumber)
    Assertions.assertEquals(
      "${connectionId}_${jobId}_${attemptNumber}_sync",
      generatedWorkloadId,
    )
  }

  companion object {
    @JvmStatic
    private fun workloadIdArgsMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(UUID.randomUUID(), 12412431L.toString(), 1),
        Arguments.of(UUID.randomUUID(), "89127421", 2),
        Arguments.of(UUID.randomUUID(), UUID.randomUUID().toString(), 0),
        Arguments.of(UUID.randomUUID(), "any string really", 0),
      )

    @JvmStatic
    private fun actorTimestampMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(UUID.randomUUID(), System.currentTimeMillis() + 12412431L),
        Arguments.of(UUID.randomUUID(), 89127421L),
        Arguments.of(UUID.randomUUID(), 0),
        Arguments.of(UUID.randomUUID(), System.currentTimeMillis()),
        Arguments.of(UUID.randomUUID(), System.currentTimeMillis() - 12412431L),
      )

    @JvmStatic
    private fun windowSnapMatrix(): Stream<Arguments> {
      val oneMinMs = 60000
      val tenMinMs = 600000
      val fifteenMinMs = 900000
      val thirtyMinMs = 1800000

      return Stream.of(
        Arguments.of(timestampMs(16, 0, 40), timestampMs(16, 0, 0), oneMinMs),
        Arguments.of(timestampMs(15, 59, 59), timestampMs(15, 59, 0), oneMinMs),
        Arguments.of(timestampMs(0, 0, 0), timestampMs(0, 0, 0), oneMinMs),
        Arguments.of(timestampMs(0, 0, 1), timestampMs(0, 0, 0), oneMinMs),
        Arguments.of(timestampMs(16, 0, 40), timestampMs(16, 0, 0), tenMinMs),
        Arguments.of(timestampMs(15, 59, 59), timestampMs(15, 50, 0), tenMinMs),
        Arguments.of(timestampMs(0, 0, 0), timestampMs(0, 0, 0), tenMinMs),
        Arguments.of(timestampMs(0, 0, 1), timestampMs(0, 0, 0), tenMinMs),
        Arguments.of(timestampMs(3, 16, 52), timestampMs(3, 10, 0), tenMinMs),
        Arguments.of(timestampMs(6, 9, 11), timestampMs(6, 0, 0), tenMinMs),
        Arguments.of(timestampMs(16, 0, 40), timestampMs(16, 0, 0), fifteenMinMs),
        Arguments.of(timestampMs(15, 59, 59), timestampMs(15, 45, 0), fifteenMinMs),
        Arguments.of(timestampMs(0, 0, 0), timestampMs(0, 0, 0), fifteenMinMs),
        Arguments.of(timestampMs(0, 0, 1), timestampMs(0, 0, 0), fifteenMinMs),
        Arguments.of(timestampMs(3, 16, 52), timestampMs(3, 15, 0), fifteenMinMs),
        Arguments.of(timestampMs(6, 9, 11), timestampMs(6, 0, 0), fifteenMinMs),
        Arguments.of(timestampMs(6, 39, 11), timestampMs(6, 30, 0), fifteenMinMs),
        Arguments.of(timestampMs(16, 0, 40), timestampMs(16, 0, 0), thirtyMinMs),
        Arguments.of(timestampMs(15, 59, 59), timestampMs(15, 30, 0), thirtyMinMs),
        Arguments.of(timestampMs(0, 0, 0), timestampMs(0, 0, 0), thirtyMinMs),
        Arguments.of(timestampMs(0, 0, 1), timestampMs(0, 0, 0), thirtyMinMs),
        Arguments.of(timestampMs(3, 16, 52), timestampMs(3, 0, 0), thirtyMinMs),
        Arguments.of(timestampMs(6, 9, 11), timestampMs(6, 0, 0), thirtyMinMs),
        Arguments.of(timestampMs(6, 39, 11), timestampMs(6, 30, 0), thirtyMinMs),
      )
    }

    private fun timestampMs(
      hr: Int,
      min: Int,
      sec: Int,
    ): Long =
      OffsetDateTime
        .now()
        .withHour(hr)
        .withMinute(min)
        .withSecond(sec)
        .withNano(0) // zero this out so we don't get remainders
        .toInstant()
        .toEpochMilli()
  }
}
