/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.model.generated.StreamStatusJobType
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusRunState
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.server.repositories.StreamStatusesRepository
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams
import io.airbyte.server.repositories.domain.StreamStatus.StreamStatusBuilder
import io.airbyte.server.repositories.domain.StreamStatusRateLimitedMetadataRepositoryStructure
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

class StreamStatusesMapperTest {
  val mapper: StreamStatusesMapper = StreamStatusesMapper()

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  internal inner class ApiToDomain {
    @Test
    fun mapRunState() {
      Assertions.assertEquals(JobStreamStatusRunState.pending, mapper.map(StreamStatusRunState.PENDING))
      Assertions.assertEquals(JobStreamStatusRunState.running, mapper.map(StreamStatusRunState.RUNNING))
      Assertions.assertEquals(JobStreamStatusRunState.complete, mapper.map(StreamStatusRunState.COMPLETE))
      Assertions.assertEquals(JobStreamStatusRunState.incomplete, mapper.map(StreamStatusRunState.INCOMPLETE))
      Assertions.assertEquals(JobStreamStatusRunState.rate_limited, mapper.map(StreamStatusRunState.RATE_LIMITED))
    }

    @Test
    fun mapIncompleteCause() {
      Assertions.assertEquals(JobStreamStatusIncompleteRunCause.failed, mapper.map(StreamStatusIncompleteRunCause.FAILED))
      Assertions.assertEquals(JobStreamStatusIncompleteRunCause.canceled, mapper.map(StreamStatusIncompleteRunCause.CANCELED))
    }

    @Test
    fun mapJobType() {
      Assertions.assertEquals(JobStreamStatusJobType.sync, mapper.map(StreamStatusJobType.SYNC))
      Assertions.assertEquals(JobStreamStatusJobType.reset, mapper.map(StreamStatusJobType.RESET))
    }

    @ParameterizedTest
    @MethodSource("paginationMatrix")
    fun mapPagination(
      pageSize: Int,
      apiRowOffset: Int,
      domainPageOffset: Int,
    ) {
      val api = Pagination().rowOffset(apiRowOffset).pageSize(pageSize)
      val domain = StreamStatusesRepository.Pagination(domainPageOffset, pageSize)
      Assertions.assertEquals(domain, mapper.map(api))
    }

    @ParameterizedTest
    @ArgumentsSource(FilterParamsMatrix::class)
    fun mapListFilters(
      workspaceId: UUID?,
      connectionId: UUID?,
      jobId: Long?,
      attemptNumber: Int?,
      streamNamespace: String?,
      streamName: String?,
      jobType: StreamStatusJobType?,
    ) {
      val pagination = Pagination().pageSize(10).rowOffset(0)
      val api =
        StreamStatusListRequestBody()
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .jobType(jobType)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .pagination(pagination)
      val domain =
        FilterParams(
          workspaceId,
          connectionId,
          jobId,
          streamNamespace,
          streamName,
          attemptNumber,
          if (jobType != null) mapper.map(jobType) else null,
          mapper.map(pagination),
        )

      Assertions.assertEquals(domain, mapper.map(api))
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix::class)
    fun mapStreamStatusCreate(
      workspaceId: UUID?,
      connectionId: UUID?,
      jobId: Long?,
      attemptNumber: Int?,
      streamNamespace: String?,
      streamName: String?,
    ) {
      val transitionedAt = System.currentTimeMillis()
      val api =
        StreamStatusCreateRequestBody()
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
          .metadata(StreamStatusRateLimitedMetadata().quotaReset(transitionedAt))
      val domain =
        StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), ZoneOffset.UTC))
          .metadata(Jsons.jsonNode(StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .build()

      val mapped = mapper.map(api)

      // test fields individually for debuggability
      Assertions.assertEquals(domain.runState, mapped.runState)
      Assertions.assertEquals(domain.incompleteRunCause, mapped.incompleteRunCause)
      Assertions.assertEquals(domain.jobType, mapped.jobType)
      Assertions.assertEquals(domain.workspaceId, mapped.workspaceId)
      Assertions.assertEquals(domain.connectionId, mapped.connectionId)
      Assertions.assertEquals(domain.jobId, mapped.jobId)
      Assertions.assertEquals(domain.attemptNumber, mapped.attemptNumber)
      Assertions.assertEquals(domain.streamNamespace, mapped.streamNamespace)
      Assertions.assertEquals(domain.streamName, mapped.streamName)
      Assertions.assertEquals(domain.transitionedAt, mapped.transitionedAt)
      Assertions.assertEquals(domain.metadata, mapped.metadata)
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(domain, mapped)
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix::class)
    fun mapStreamStatusUpdate(
      workspaceId: UUID?,
      connectionId: UUID?,
      jobId: Long?,
      attemptNumber: Int?,
      streamNamespace: String?,
      streamName: String?,
    ) {
      val id = UUID.randomUUID()
      val transitionedAt = System.currentTimeMillis()
      val api =
        StreamStatusUpdateRequestBody()
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
          .metadata(StreamStatusRateLimitedMetadata().quotaReset(transitionedAt))
          .id(id)
      val domain =
        StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), ZoneOffset.UTC))
          .metadata(Jsons.jsonNode(StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .id(id)
          .build()

      val mapped = mapper.map(api)

      // test fields individually for debuggability
      Assertions.assertEquals(domain.runState, mapped.runState)
      Assertions.assertEquals(domain.incompleteRunCause, mapped.incompleteRunCause)
      Assertions.assertEquals(domain.jobType, mapped.jobType)
      Assertions.assertEquals(domain.workspaceId, mapped.workspaceId)
      Assertions.assertEquals(domain.connectionId, mapped.connectionId)
      Assertions.assertEquals(domain.jobId, mapped.jobId)
      Assertions.assertEquals(domain.attemptNumber, mapped.attemptNumber)
      Assertions.assertEquals(domain.streamNamespace, mapped.streamNamespace)
      Assertions.assertEquals(domain.streamName, mapped.streamName)
      Assertions.assertEquals(domain.transitionedAt, mapped.transitionedAt)
      Assertions.assertEquals(domain.id, mapped.id)
      Assertions.assertEquals(domain.metadata, mapped.metadata)
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(domain, mapped)
    }

    private fun paginationMatrix() =
      listOf(
        Arguments.of(10, 0, 0),
        Arguments.of(10, 20, 2),
        Arguments.of(5, 40, 8),
        Arguments.of(100, 100, 1),
        Arguments.of(23, 161, 7),
      )
  }

  @Nested
  internal inner class DomainToApi {
    @Test
    fun mapRunState() {
      Assertions.assertEquals(StreamStatusRunState.PENDING, mapper.map(JobStreamStatusRunState.pending))
      Assertions.assertEquals(StreamStatusRunState.RUNNING, mapper.map(JobStreamStatusRunState.running))
      Assertions.assertEquals(StreamStatusRunState.COMPLETE, mapper.map(JobStreamStatusRunState.complete))
      Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, mapper.map(JobStreamStatusRunState.incomplete))
      Assertions.assertEquals(StreamStatusRunState.RATE_LIMITED, mapper.map(JobStreamStatusRunState.rate_limited))
    }

    @Test
    fun mapIncompleteCause() {
      Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, mapper.map(JobStreamStatusIncompleteRunCause.failed))
      Assertions.assertEquals(StreamStatusIncompleteRunCause.CANCELED, mapper.map(JobStreamStatusIncompleteRunCause.canceled))
    }

    @Test
    fun mapJobType() {
      Assertions.assertEquals(StreamStatusJobType.SYNC, mapper.map(JobStreamStatusJobType.sync))
      Assertions.assertEquals(StreamStatusJobType.RESET, mapper.map(JobStreamStatusJobType.reset))
    }

    @ParameterizedTest
    @ArgumentsSource(StreamStatusParamsMatrix::class)
    fun mapStreamStatusRead(
      workspaceId: UUID?,
      connectionId: UUID?,
      jobId: Long?,
      attemptNumber: Int?,
      streamNamespace: String?,
      streamName: String?,
    ) {
      val id = UUID.randomUUID()
      val transitionedAt = System.currentTimeMillis()
      val domain =
        StreamStatusBuilder()
          .runState(mapper.map(StreamStatusRunState.INCOMPLETE))
          .incompleteRunCause(mapper.map(StreamStatusIncompleteRunCause.FAILED))
          .jobType(mapper.map(StreamStatusJobType.SYNC))
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .attemptNumber(attemptNumber)
          .streamNamespace(streamNamespace)
          .streamName(streamName)
          .transitionedAt(OffsetDateTime.ofInstant(Instant.ofEpochMilli(transitionedAt), ZoneOffset.UTC))
          .id(id)
          .metadata(Jsons.jsonNode(StreamStatusRateLimitedMetadataRepositoryStructure(transitionedAt)))
          .build()
      val api =
        StreamStatusRead()
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
          .metadata(StreamStatusRateLimitedMetadata().quotaReset(transitionedAt))
          .id(id)

      val mapped = mapper.map(domain)

      // test fields individually for debuggability
      Assertions.assertEquals(api.runState, mapped.runState)
      Assertions.assertEquals(api.incompleteRunCause, mapped.incompleteRunCause)
      Assertions.assertEquals(api.jobType, mapped.jobType)
      Assertions.assertEquals(api.workspaceId, mapped.workspaceId)
      Assertions.assertEquals(api.connectionId, mapped.connectionId)
      Assertions.assertEquals(api.jobId, mapped.jobId)
      Assertions.assertEquals(api.attemptNumber, mapped.attemptNumber)
      Assertions.assertEquals(api.streamNamespace, mapped.streamNamespace)
      Assertions.assertEquals(api.streamName, mapped.streamName)
      Assertions.assertEquals(api.transitionedAt, mapped.transitionedAt)
      Assertions.assertEquals(api.id, mapped.id)
      Assertions.assertEquals(api.metadata, mapped.metadata)
      // test whole shebang in case we forget to add fields above
      Assertions.assertEquals(api, mapped)
    }
  }

  internal class StreamStatusParamsMatrix : ArgumentsProvider {
    override fun provideArguments(unused: ExtensionContext) =
      listOf(
        Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId1, 0, Fixtures.testNamespace1, Fixtures.testName1),
        Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 1, Fixtures.testNamespace1, Fixtures.testName1),
        Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId1, Fixtures.jobId2, 0, "", Fixtures.testName3),
        Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId3, Fixtures.jobId2, 1, Fixtures.testNamespace2, Fixtures.testName2),
        Arguments.of(Fixtures.workspaceId1, Fixtures.connectionId2, Fixtures.jobId3, 2, Fixtures.testNamespace2, Fixtures.testName2),
        Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, Fixtures.jobId1, 1, Fixtures.testNamespace1, Fixtures.testName1),
      ).stream()
  }

  internal class FilterParamsMatrix : ArgumentsProvider {
    override fun provideArguments(unused: ExtensionContext) =
      listOf(
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
        Arguments.of(
          Fixtures.workspaceId1,
          Fixtures.connectionId2,
          Fixtures.jobId3,
          null,
          Fixtures.testNamespace2,
          Fixtures.testName2,
          RESET,
        ),
        Arguments.of(Fixtures.workspaceId3, Fixtures.connectionId2, null, null, null, Fixtures.testName1, SYNC),
        Arguments.of(Fixtures.workspaceId2, null, Fixtures.jobId2, null, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
        Arguments.of(Fixtures.workspaceId2, null, Fixtures.jobId2, null, Fixtures.testNamespace1, Fixtures.testName1, RESET),
        Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, null, null, Fixtures.testNamespace1, Fixtures.testName1, SYNC),
        Arguments.of(Fixtures.workspaceId2, Fixtures.connectionId3, null, null, Fixtures.testNamespace1, Fixtures.testName1, RESET),
      ).stream()

    companion object {
      // shave some characters, so reformatting doesn't wrap lines below
      val SYNC: StreamStatusJobType = StreamStatusJobType.SYNC
      val RESET: StreamStatusJobType = StreamStatusJobType.RESET
    }
  }

  internal object Fixtures {
    var testNamespace1: String = "test_"
    var testNamespace2: String = "other_"

    var testName1: String = "table_1"
    var testName2: String = "table_2"
    var testName3: String = "table_3"

    var workspaceId1: UUID = UUID.randomUUID()
    var workspaceId2: UUID = UUID.randomUUID()
    var workspaceId3: UUID = UUID.randomUUID()

    var connectionId1: UUID = UUID.randomUUID()
    var connectionId2: UUID = UUID.randomUUID()
    var connectionId3: UUID = UUID.randomUUID()

    var jobId1: Long = ThreadLocalRandom.current().nextLong()
    var jobId2: Long = ThreadLocalRandom.current().nextLong()
    var jobId3: Long = ThreadLocalRandom.current().nextLong()
  }
}
