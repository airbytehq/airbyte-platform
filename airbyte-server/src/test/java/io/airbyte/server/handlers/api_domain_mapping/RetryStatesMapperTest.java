/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.server.repositories.domain.RetryState;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RetryStatesMapperTest {

  final RetryStatesMapper mapper = new RetryStatesMapper();

  @ParameterizedTest
  @MethodSource("retryStateFieldsMatrix")
  void mapsRequestToRetryState(
                               final UUID unused,
                               final Long jobId,
                               final UUID connectionId,
                               final Integer successiveCompleteFailures,
                               final Integer totalCompleteFailures,
                               final Integer successivePartialFailures,
                               final Integer totalPartialFailures) {
    final var api = new JobRetryStateRequestBody()
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures);

    final var expected = new RetryState.RetryStateBuilder()
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)
        .build();

    final var result = mapper.map(api);

    Assertions.assertEquals(expected, result);
  }

  @ParameterizedTest
  @MethodSource("retryStateFieldsMatrix")
  void mapsRetryStateToRead(
                            final UUID retryId,
                            final Long jobId,
                            final UUID connectionId,
                            final Integer successiveCompleteFailures,
                            final Integer totalCompleteFailures,
                            final Integer successivePartialFailures,
                            final Integer totalPartialFailures) {
    final var domain = new RetryState.RetryStateBuilder()
        .id(retryId)
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures)
        .build();

    final var expected = new RetryStateRead()
        .id(retryId)
        .jobId(jobId)
        .connectionId(connectionId)
        .successiveCompleteFailures(successiveCompleteFailures)
        .totalCompleteFailures(totalCompleteFailures)
        .successivePartialFailures(successivePartialFailures)
        .totalPartialFailures(totalPartialFailures);

    final var result = mapper.map(domain);

    Assertions.assertEquals(expected, result);
  }

  public static Stream<Arguments> retryStateFieldsMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.retryId1, Fixtures.jobId1, Fixtures.connectionId1, 1, 2, 3, 4),
        Arguments.of(Fixtures.retryId2, Fixtures.jobId1, Fixtures.connectionId1, 0, 0, 9, 9),
        Arguments.of(Fixtures.retryId3, Fixtures.jobId2, Fixtures.connectionId2, 3, 2, 1, 0),
        Arguments.of(Fixtures.retryId2, Fixtures.jobId3, Fixtures.connectionId1, 3, 2, 1, 9),
        Arguments.of(Fixtures.retryId1, Fixtures.jobId1, Fixtures.connectionId2, 1, 1, 0, 0));
  }

  private static class Fixtures {

    static UUID retryId1 = UUID.randomUUID();
    static UUID retryId2 = UUID.randomUUID();
    static UUID retryId3 = UUID.randomUUID();

    static Long jobId1 = ThreadLocalRandom.current().nextLong();
    static Long jobId2 = ThreadLocalRandom.current().nextLong();
    static Long jobId3 = ThreadLocalRandom.current().nextLong();

    static UUID connectionId1 = UUID.randomUUID();
    static UUID connectionId2 = UUID.randomUUID();

  }

}
