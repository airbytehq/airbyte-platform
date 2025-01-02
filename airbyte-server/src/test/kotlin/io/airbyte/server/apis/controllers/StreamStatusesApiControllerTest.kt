/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.model.generated.StreamStatusJobType
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusReadList
import io.airbyte.api.model.generated.StreamStatusRunState
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.server.assertStatus
import io.airbyte.server.handlers.StreamStatusesHandler
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Stream
import kotlin.random.Random

private const val PATH_BASE = "/api/v1/stream_statuses"
private const val PATH_CREATE = "$PATH_BASE/create"
private const val PATH_UPDATE = "$PATH_BASE/update"
private const val PATH_LIST = "$PATH_BASE/list"
private const val PATH_LATEST_PER_RUN_STATE = "$PATH_BASE/latest_per_run_state"

@MicronautTest
internal class StreamStatusesApiControllerTest {
  @Inject
  lateinit var handler: StreamStatusesHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(StreamStatusesHandler::class)
  fun streamStatusesHandler(): StreamStatusesHandler = mockk()

  @Test
  fun testCreateSuccessful() {
    every { handler.createStreamStatus(any()) } returns StreamStatusRead()

    assertStatus(HttpStatus.CREATED, client.status(HttpRequest.POST(PATH_CREATE, validCreate())))
  }

  @ParameterizedTest
  @MethodSource("invalidRunStateCauseMatrix")
  fun testCreateIncompleteRunCauseRunStateInvariant(
    state: StreamStatusRunState?,
    incompleteCause: StreamStatusIncompleteRunCause?,
  ) {
    every { handler.createStreamStatus(any()) } returns StreamStatusRead()

    val invalid =
      validCreate()
        .runState(state)
        .incompleteRunCause(incompleteCause)

    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(PATH_CREATE, invalid)))
  }

  @Test
  fun testUpdateSuccessful() {
    every { handler.updateStreamStatus(any()) } returns StreamStatusRead()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(PATH_UPDATE, validUpdate())))
  }

  @ParameterizedTest
  @MethodSource("invalidRunStateCauseMatrix")
  fun testUpdateIncompleteRunCauseRunStateInvariant(
    state: StreamStatusRunState?,
    incompleteCause: StreamStatusIncompleteRunCause?,
  ) {
    every { handler.updateStreamStatus(any()) } returns StreamStatusRead()

    val invalid =
      validUpdate()
        .runState(state)
        .incompleteRunCause(incompleteCause)

    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(PATH_UPDATE, invalid)))
  }

  @ParameterizedTest
  @MethodSource("validPaginationMatrix")
  fun testListSuccessful(pagination: Pagination?) {
    every { handler.listStreamStatus(any()) } returns StreamStatusReadList()

    val valid = validList().pagination(pagination)

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(PATH_LIST, valid)))
  }

  @ParameterizedTest
  @MethodSource("invalidListPaginationMatrix")
  fun testListInvalidPagination(invalidPagination: Pagination?) {
    every { handler.listStreamStatus(any()) } returns StreamStatusReadList()

    val invalid = validList().pagination(invalidPagination)

    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(PATH_LIST, invalid)))
  }

  @Test
  fun testListPerRunStateSuccessful() {
    val req = ConnectionIdRequestBody().connectionId(UUID.randomUUID())

    every { handler.listStreamStatusPerRunState(req) } returns StreamStatusReadList()

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(PATH_LATEST_PER_RUN_STATE, req)))
  }

  companion object {
    @JvmStatic
    private fun invalidRunStateCauseMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(StreamStatusRunState.PENDING, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.PENDING, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.RUNNING, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.RUNNING, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.COMPLETE, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.COMPLETE, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.INCOMPLETE, null),
      )

    @JvmStatic
    private fun validPaginationMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(validPagination()),
        Arguments.of(validPagination().rowOffset(30)),
        Arguments.of(validPagination().pageSize(100).rowOffset(300)),
        Arguments.of(validPagination().pageSize(5).rowOffset(10)),
      )

    @JvmStatic
    private fun invalidListPaginationMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(null as Pagination?),
        Arguments.of(validPagination().pageSize(0)),
        Arguments.of(validPagination().pageSize(-1)),
        Arguments.of(validPagination().rowOffset(-1)),
        Arguments.of(validPagination().pageSize(-1).rowOffset(-1)),
        Arguments.of(validPagination().pageSize(0).rowOffset(-1)),
        Arguments.of(validPagination().pageSize(10).rowOffset(23)),
        Arguments.of(validPagination().pageSize(20).rowOffset(10)),
        Arguments.of(validPagination().pageSize(100).rowOffset(50)),
      )
  }
}

private val testNamespace = "test_"
private val testName = "table_1"
private val workspaceId = UUID.randomUUID()
private val connectionId = UUID.randomUUID()
private val jobId: Long = Random.nextLong()
private val transitionedAtMs: Long = System.currentTimeMillis()

private fun validCreate(): StreamStatusCreateRequestBody =
  StreamStatusCreateRequestBody()
    .workspaceId(workspaceId)
    .connectionId(connectionId)
    .jobId(jobId)
    .jobType(StreamStatusJobType.SYNC)
    .attemptNumber(0)
    .streamNamespace(testNamespace)
    .streamName(testName)
    .runState(StreamStatusRunState.PENDING)
    .transitionedAt(transitionedAtMs)

private fun validUpdate(): StreamStatusUpdateRequestBody =
  StreamStatusUpdateRequestBody()
    .workspaceId(workspaceId)
    .connectionId(connectionId)
    .jobId(jobId)
    .jobType(StreamStatusJobType.SYNC)
    .attemptNumber(0)
    .streamNamespace(testNamespace)
    .streamName(testName)
    .runState(StreamStatusRunState.PENDING)
    .transitionedAt(transitionedAtMs)
    .id(UUID.randomUUID())

private fun validPagination(): Pagination =
  Pagination()
    .pageSize(10)
    .rowOffset(0)

fun validList(): StreamStatusListRequestBody =
  StreamStatusListRequestBody()
    .workspaceId(UUID.randomUUID())
    .jobId(ThreadLocalRandom.current().nextLong())
    .pagination(validPagination())
