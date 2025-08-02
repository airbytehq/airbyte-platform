/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.server.repositories.domain.RetryState
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Stream

internal class RetryStatesMapperTest {
  val mapper: RetryStatesMapper = RetryStatesMapper()

  @ParameterizedTest
  @MethodSource("retryStateFieldsMatrix")
  fun mapsRequestToRetryState(
    unused: UUID?,
    jobId: Long?,
    connectionId: UUID?,
    successiveCompleteFailures: Int?,
    totalCompleteFailures: Int?,
    successivePartialFailures: Int?,
    totalPartialFailures: Int?,
  ) {
    val api =
      JobRetryStateRequestBody()
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)

    val expected =
      RetryState
        .RetryStateBuilder()
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)
        .build()

    val result = mapper.map(api)

    Assertions.assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("retryStateFieldsMatrix")
  fun mapsRetryStateToRead(
    retryId: UUID?,
    jobId: Long?,
    connectionId: UUID?,
    successiveCompleteFailures: Int?,
    totalCompleteFailures: Int?,
    successivePartialFailures: Int?,
    totalPartialFailures: Int?,
  ) {
    val domain =
      RetryState
        .RetryStateBuilder()
        .id(retryId)
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)
        .build()

    val expected =
      RetryStateRead()
        .id(retryId)
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)

    val result = mapper.map(domain)

    Assertions.assertEquals(expected, result)
  }

  private object Fixtures {
    var retryId1: UUID = UUID.randomUUID()
    var retryId2: UUID = UUID.randomUUID()
    var retryId3: UUID = UUID.randomUUID()

    var jobId1: Long = ThreadLocalRandom.current().nextLong()
    var jobId2: Long = ThreadLocalRandom.current().nextLong()
    var jobId3: Long = ThreadLocalRandom.current().nextLong()

    var connectionId1: UUID = UUID.randomUUID()
    var connectionId2: UUID = UUID.randomUUID()
  }

  companion object {
    @JvmStatic
    fun retryStateFieldsMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(Fixtures.retryId1, Fixtures.jobId1, Fixtures.connectionId1, 1, 2, 3, 4),
        Arguments.of(Fixtures.retryId2, Fixtures.jobId1, Fixtures.connectionId1, 0, 0, 9, 9),
        Arguments.of(Fixtures.retryId3, Fixtures.jobId2, Fixtures.connectionId2, 3, 2, 1, 0),
        Arguments.of(Fixtures.retryId2, Fixtures.jobId3, Fixtures.connectionId1, 3, 2, 1, 9),
        Arguments.of(Fixtures.retryId1, Fixtures.jobId1, Fixtures.connectionId2, 1, 1, 0, 0),
      )
  }
}
