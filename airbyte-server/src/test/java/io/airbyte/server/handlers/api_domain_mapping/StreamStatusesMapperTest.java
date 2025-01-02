/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import static java.time.ZoneOffset.UTC;

import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusJobType;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRateLimitedMetadata;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.airbyte.server.repositories.StreamStatusesRepository;
import io.airbyte.server.repositories.domain.StreamStatus;
import io.airbyte.server.repositories.domain.StreamStatusRateLimitedMetadataRepositoryStructure;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

public class StreamStatusesMapperTest {

  final StreamStatusesMapper mapper = new StreamStatusesMapper();

  @Nested
  class ApiToDomain {

    @Test
    void mapRunState() {
      Assertions.assertEquals(JobStreamStatusRunState.pending, mapper.map(StreamStatusRunState.PENDING));
      Assertions.assertEquals(JobStreamStatusRunState.running, mapper.map(StreamStatusRunState.RUNNING));
      Assertions.assertEquals(JobStreamStatusRunState.complete, mapper.map(StreamStatusRunState.COMPLETE));
      Assertions.assertEquals(JobStreamStatusRunState.incomplete, mapper.map(StreamStatusRunState.INCOMPLETE));
      Assertions.assertEquals(JobStreamStatusRunState.rate_limited, mapper.map(StreamStatusRunState.RATE_LIMITED));
    }

    @Test
    void mapIncompleteCause() {
      Assertions.assertEquals(JobStreamStatusIncompleteRunCause.failed, mapper.map(StreamStatusIncompleteRunCause.FAILED));
      Assertions.assertEquals(JobStreamStatusIncompleteRunCause.canceled, mapper.map(StreamStatusIncompleteRunCause.CANCELED));
    }

    @Test
    void mapJobType() {
      Assertions.assertEquals(JobStreamStatusJobType.sync, mapper.map(StreamStatusJobType.SYNC));
      Assertions.assertEquals(JobStreamStatusJobType.reset, mapper.map(StreamStatusJobType.RESET));
    }

    @ParameterizedTest
    @MethodSource("paginationMatrix")
    void mapPagination(final int pageSize, final int apiRowOffset, final int domainPageOffset) {
      final var api = new Pagination().rowOffset(apiRowOffset).pageSize(pageSize);
      final var domain = new StreamStatusesRepository.Pagination(domainPageOffset, pageSize);
      Assertions.assertEquals(domain, mapper.map(api));
    }

    private static Stream<Arguments> paginationMatrix() {
      return Stream.of(
          Arguments.of(10, 0, 0),
          Arguments.of(10, 20, 2),
          Arguments.of(5, 40, 8),
          Arguments.of(100, 100, 1),
          Arguments.of(23, 161, 7));
    }

    @ParameterizedTest
    @ArgumentsSource(FilterParamsMatrix.class)
    void mapListFilters(
                        final UUID workspaceId,
                        final UUID connectionId,
                        final Long jobId,
                        final Integer attemptNumber,
                        final String streamNamespace,
                        final String streamName,
                        final StreamStatusJobType jobType) {
      final var pagination = new Pagination().pageSize(10).rowOffset(0);
      final var api = new StreamStatusListRequestBody()
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .jobType(jobType)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .pagination(pagination);
      final var domain = new StreamStatusesRepository.FilterParams(
          workspaceId,
          connectionId,
          jobId,
          streamNamespace,
          streamName,
          attemptNumber,
          jobType != null ? mapper.map(jobType) : null,
          mapper.map(pagination));

      Assertions.assertEquals(domain, mapper.map(api));
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix.class)
    void mapStreamStatusCreate(
                               final UUID workspaceId,
                               final UUID connectionId,
                               final Long jobId,
                               final Integer attemptNumber,
                               final String streamNamespace,
                               final String streamName) {
      final long transitionedAt = System.currentTimeMillis();
      final var api = new StreamStatusCreateRequestBody()
          .runState(StreamStatusRunState.INCOMPLETE)
          .incompleteRunCause(StreamStatusIncompleteRunCause.FAILED)
          .jobType(StreamStatusJobType.SYNC)
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(transitionedAt)
          .metadata(new StreamStatusRateLimitedMetadata().quotaReset(transitionedAt));
      final var domain = new StreamStatus.StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), UTC))
          .metadata(Jsons.jsonNode(new StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .build();

      final var mapped = mapper.map(api);

      // test fields individually for debuggability
      Assertions.assertEquals(domain.getRunState(), mapped.getRunState());
      Assertions.assertEquals(domain.getIncompleteRunCause(), mapped.getIncompleteRunCause());
      Assertions.assertEquals(domain.getJobType(), mapped.getJobType());
      Assertions.assertEquals(domain.getWorkspaceId(), mapped.getWorkspaceId());
      Assertions.assertEquals(domain.getConnectionId(), mapped.getConnectionId());
      Assertions.assertEquals(domain.getJobId(), mapped.getJobId());
      Assertions.assertEquals(domain.getAttemptNumber(), mapped.getAttemptNumber());
      Assertions.assertEquals(domain.getStreamNamespace(), mapped.getStreamNamespace());
      Assertions.assertEquals(domain.getStreamName(), mapped.getStreamName());
      Assertions.assertEquals(domain.getTransitionedAt(), mapped.getTransitionedAt());
      Assertions.assertEquals(domain.getMetadata(), mapped.getMetadata());
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(domain, mapped);
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix.class)
    void mapStreamStatusUpdate(
                               final UUID workspaceId,
                               final UUID connectionId,
                               final Long jobId,
                               final Integer attemptNumber,
                               final String streamNamespace,
                               final String streamName) {
      final UUID id = UUID.randomUUID();
      final long transitionedAt = System.currentTimeMillis();
      final var api = new StreamStatusUpdateRequestBody()
          .runState(StreamStatusRunState.INCOMPLETE)
          .incompleteRunCause(StreamStatusIncompleteRunCause.FAILED)
          .jobType(StreamStatusJobType.SYNC)
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(transitionedAt)
          .metadata(new StreamStatusRateLimitedMetadata().quotaReset(transitionedAt))
          .id(id);
      final var domain = new StreamStatus.StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), UTC))
          .metadata(Jsons.jsonNode(new StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .id(id)
          .build();

      final var mapped = mapper.map(api);

      // test fields individually for debuggability
      Assertions.assertEquals(domain.getRunState(), mapped.getRunState());
      Assertions.assertEquals(domain.getIncompleteRunCause(), mapped.getIncompleteRunCause());
      Assertions.assertEquals(domain.getJobType(), mapped.getJobType());
      Assertions.assertEquals(domain.getWorkspaceId(), mapped.getWorkspaceId());
      Assertions.assertEquals(domain.getConnectionId(), mapped.getConnectionId());
      Assertions.assertEquals(domain.getJobId(), mapped.getJobId());
      Assertions.assertEquals(domain.getAttemptNumber(), mapped.getAttemptNumber());
      Assertions.assertEquals(domain.getStreamNamespace(), mapped.getStreamNamespace());
      Assertions.assertEquals(domain.getStreamName(), mapped.getStreamName());
      Assertions.assertEquals(domain.getTransitionedAt(), mapped.getTransitionedAt());
      Assertions.assertEquals(domain.getId(), mapped.getId());
      Assertions.assertEquals(domain.getMetadata(), mapped.getMetadata());
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(domain, mapped);
    }

  }

  @Nested
  class DomainToApi {

    @Test
    void mapRunState() {
      Assertions.assertEquals(StreamStatusRunState.PENDING, mapper.map(JobStreamStatusRunState.pending));
      Assertions.assertEquals(StreamStatusRunState.RUNNING, mapper.map(JobStreamStatusRunState.running));
      Assertions.assertEquals(StreamStatusRunState.COMPLETE, mapper.map(JobStreamStatusRunState.complete));
      Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, mapper.map(JobStreamStatusRunState.incomplete));
      Assertions.assertEquals(StreamStatusRunState.RATE_LIMITED, mapper.map(JobStreamStatusRunState.rate_limited));
    }

    @Test
    void mapIncompleteCause() {
      Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, mapper.map(JobStreamStatusIncompleteRunCause.failed));
      Assertions.assertEquals(StreamStatusIncompleteRunCause.CANCELED, mapper.map(JobStreamStatusIncompleteRunCause.canceled));
    }

    @Test
    void mapJobType() {
      Assertions.assertEquals(StreamStatusJobType.SYNC, mapper.map(JobStreamStatusJobType.sync));
      Assertions.assertEquals(StreamStatusJobType.RESET, mapper.map(JobStreamStatusJobType.reset));
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix.class)
    void mapStreamStatusRead(
                             final UUID workspaceId,
                             final UUID connectionId,
                             final Long jobId,
                             final Integer attemptNumber,
                             final String streamNamespace,
                             final String streamName) {
      final UUID id = UUID.randomUUID();
      final long transitionedAt = System.currentTimeMillis();
      final var domain = new StreamStatus.StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), UTC))
          .id(id)
          .metadata(Jsons.jsonNode(new StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .build();
      final var api = new StreamStatusRead()
          .runState(StreamStatusRunState.INCOMPLETE)
          .incompleteRunCause(StreamStatusIncompleteRunCause.FAILED)
          .jobType(StreamStatusJobType.SYNC)
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(transitionedAt)
          .metadata(new StreamStatusRateLimitedMetadata().quotaReset(transitionedAt))
          .id(id);

      final var mapped = mapper.map(domain);

      // test fields individually for debuggability
      Assertions.assertEquals(api.getRunState(), mapped.getRunState());
      Assertions.assertEquals(api.getIncompleteRunCause(), mapped.getIncompleteRunCause());
      Assertions.assertEquals(api.getJobType(), mapped.getJobType());
      Assertions.assertEquals(api.getWorkspaceId(), mapped.getWorkspaceId());
      Assertions.assertEquals(api.getConnectionId(), mapped.getConnectionId());
      Assertions.assertEquals(api.getJobId(), mapped.getJobId());
      Assertions.assertEquals(api.getAttemptNumber(), mapped.getAttemptNumber());
      Assertions.assertEquals(api.getStreamNamespace(), mapped.getStreamNamespace());
      Assertions.assertEquals(api.getStreamName(), mapped.getStreamName());
      Assertions.assertEquals(api.getTransitionedAt(), mapped.getTransitionedAt());
      Assertions.assertEquals(api.getId(), mapped.getId());
      Assertions.assertEquals(api.getMetadata(), mapped.getMetadata());
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(api, mapped);
    }

  }

  static class StreamStatusParamsMatrix implements ArgumentsProvider {

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext unused) {
      return Stream.of(
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId1, 0, Fixtures.testNamespace1, Fixtures.testName1),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 1, Fixtures.testNamespace1, Fixtures.testName1),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 0, "", Fixtures.testName3),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId3, Fixtures.jobId2, 1, Fixtures.testNamespace2, Fixtures.testName2),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId2, Fixtures.jobId3, 2, Fixtures.testNamespace2, Fixtures.testName2),
          Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, Fixtures.jobId1, 1, Fixtures.testNamespace1, Fixtures.testName1));
    }

  }

  static class FilterParamsMatrix implements ArgumentsProvider {

    // shave some characters, so reformatting doesn't wrap lines below
    static StreamStatusJobType SYNC = StreamStatusJobType.SYNC;
    static StreamStatusJobType RESET = StreamStatusJobType.RESET;

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext unused) {
      return Stream.of(
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId1, 0, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 1, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 1, Fixtures.testNamespace1, Fixtures.testName1, RESET),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 0, "", Fixtures.testName3, SYNC),
          Arguments.of(Fixtures.workspaceId1, null, null, null, null, null, SYNC),
          Arguments.of(Fixtures.workspaceId2, null, null, null, null, null, null),
          Arguments.of(Fixtures.workspaceId2, null, null, null, null, null, RESET),
          Arguments.of(Fixtures.workspaceId1, null, Fixtures.jobId3, null, null, null, SYNC),
          Arguments.of(Fixtures.workspaceId1, null, Fixtures.jobId3, null, Fixtures.testNamespace2, Fixtures.testName2, SYNC),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId2, Fixtures.jobId3, null, Fixtures.testNamespace2, Fixtures.testName2, null),
          Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId2, Fixtures.jobId3, null, Fixtures.testNamespace2, Fixtures.testName2, RESET),
          Arguments.of(Fixtures.workspaceId3, Fixtures.connectionId2, null, null, null, Fixtures.testName1, SYNC),
          Arguments.of(Fixtures.workspaceId2, null, Fixtures.jobId2, null, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
          Arguments.of(Fixtures.workspaceId2, null, Fixtures.jobId2, null, Fixtures.testNamespace1, Fixtures.testName1, RESET),
          Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, null, null, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
          Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, null, null, Fixtures.testNamespace1, Fixtures.testName1, RESET));
    }

  }

  static class Fixtures {

    static String testNamespace1 = "test_";
    static String testNamespace2 = "other_";

    static String testName1 = "table_1";
    static String testName2 = "table_2";
    static String testName3 = "table_3";

    static UUID workspaceId1 = UUID.randomUUID();
    static UUID workspaceId2 = UUID.randomUUID();
    static UUID workspaceId3 = UUID.randomUUID();

    static UUID connectionId1 = UUID.randomUUID();
    static UUID connectionId2 = UUID.randomUUID();
    static UUID connectionId3 = UUID.randomUUID();

    static Long jobId1 = ThreadLocalRandom.current().nextLong();
    static Long jobId2 = ThreadLocalRandom.current().nextLong();
    static Long jobId3 = ThreadLocalRandom.current().nextLong();

  }

}
