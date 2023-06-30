/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusJobType;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.server.handlers.StreamStatusesHandler;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@MicronautTest
@Requires(property = "mockito.test.enabled",
          defaultValue = StringUtils.TRUE,
          value = StringUtils.TRUE)
@Requires(env = {Environment.TEST})
class StreamStatusesApiControllerTest extends BaseControllerTest {

  StreamStatusesHandler handler = Mockito.mock(StreamStatusesHandler.class);

  @MockBean(StreamStatusesHandler.class)
  @Replaces(StreamStatusesHandler.class)
  StreamStatusesHandler mmStreamStatusesHandler() {
    return handler;
  }

  static String PATH_BASE = "/api/v1/stream_statuses";
  static String PATH_CREATE = PATH_BASE + "/create";
  static String PATH_UPDATE = PATH_BASE + "/update";
  static String PATH_LIST = PATH_BASE + "/list";
  static String PATH_LATEST_PER_RUN_STATE = PATH_BASE + "/latest_per_run_state";

  @Test
  void testCreateSuccessful() {
    when(handler.createStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusRead());

    testEndpointStatus(
        HttpRequest.POST(
            PATH_CREATE,
            Jsons.serialize(Fixtures.validCreate())),
        HttpStatus.CREATED);
  }

  @ParameterizedTest
  @MethodSource("invalidRunStateCauseMatrix")
  void testCreateIncompleteRunCauseRunStateInvariant(final StreamStatusRunState state, final StreamStatusIncompleteRunCause incompleteCause) {
    when(handler.createStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusRead());

    final var invalid = Fixtures.validCreate()
        .runState(state)
        .incompleteRunCause(incompleteCause);

    testErrorEndpointStatus(
        HttpRequest.POST(
            PATH_CREATE,
            Jsons.serialize(invalid)),
        HttpStatus.BAD_REQUEST);
  }

  @Test
  void testUpdateSuccessful() {
    when(handler.updateStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusRead());

    testEndpointStatus(
        HttpRequest.POST(
            PATH_UPDATE,
            Jsons.serialize(Fixtures.validUpdate())),
        HttpStatus.OK);
  }

  @ParameterizedTest
  @MethodSource("invalidRunStateCauseMatrix")
  void testUpdateIncompleteRunCauseRunStateInvariant(final StreamStatusRunState state, final StreamStatusIncompleteRunCause incompleteCause) {
    when(handler.updateStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusRead());

    final var invalid = Fixtures.validUpdate()
        .runState(state)
        .incompleteRunCause(incompleteCause);

    testErrorEndpointStatus(
        HttpRequest.POST(
            PATH_UPDATE,
            Jsons.serialize(invalid)),
        HttpStatus.BAD_REQUEST);
  }

  private static Stream<Arguments> invalidRunStateCauseMatrix() {
    return Stream.of(
        Arguments.of(StreamStatusRunState.PENDING, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.PENDING, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.RUNNING, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.RUNNING, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.COMPLETE, StreamStatusIncompleteRunCause.FAILED),
        Arguments.of(StreamStatusRunState.COMPLETE, StreamStatusIncompleteRunCause.CANCELED),
        Arguments.of(StreamStatusRunState.INCOMPLETE, null));
  }

  @ParameterizedTest
  @MethodSource("validPaginationMatrix")
  void testListSuccessful(final Pagination pagination) {
    when(handler.listStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusReadList());

    final var valid = Fixtures.validList()
        .pagination(pagination);

    testEndpointStatus(
        HttpRequest.POST(
            PATH_LIST,
            Jsons.serialize(valid)),
        HttpStatus.OK);
  }

  private static Stream<Arguments> validPaginationMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.validPagination()),
        Arguments.of(Fixtures.validPagination().rowOffset(30)),
        Arguments.of(Fixtures.validPagination().pageSize(100).rowOffset(300)),
        Arguments.of(Fixtures.validPagination().pageSize(5).rowOffset(10)));
  }

  @ParameterizedTest
  @MethodSource("invalidListPaginationMatrix")
  void testListInvalidPagination(final Pagination invalidPagination) {
    when(handler.listStreamStatus(Mockito.any()))
        .thenReturn(new StreamStatusReadList());

    final var invalid = Fixtures.validList()
        .pagination(invalidPagination);

    testErrorEndpointStatus(
        HttpRequest.POST(
            PATH_LIST,
            Jsons.serialize(invalid)),
        HttpStatus.BAD_REQUEST);
  }

  private static Stream<Arguments> invalidListPaginationMatrix() {
    return Stream.of(
        Arguments.of((Pagination) null),
        Arguments.of(Fixtures.validPagination().pageSize(0)),
        Arguments.of(Fixtures.validPagination().pageSize(-1)),
        Arguments.of(Fixtures.validPagination().rowOffset(-1)),
        Arguments.of(Fixtures.validPagination().pageSize(-1).rowOffset(-1)),
        Arguments.of(Fixtures.validPagination().pageSize(0).rowOffset(-1)),
        Arguments.of(Fixtures.validPagination().pageSize(10).rowOffset(23)),
        Arguments.of(Fixtures.validPagination().pageSize(20).rowOffset(10)),
        Arguments.of(Fixtures.validPagination().pageSize(100).rowOffset(50)));
  }

  @Test
  void testListPerRunStateSuccessful() {
    final var req = new ConnectionIdRequestBody().connectionId(UUID.randomUUID());

    when(handler.listStreamStatusPerRunState(req))
        .thenReturn(new StreamStatusReadList());

    testEndpointStatus(
        HttpRequest.POST(
            PATH_LATEST_PER_RUN_STATE,
            Jsons.serialize(req)),
        HttpStatus.OK);
  }

  static class Fixtures {

    static String testNamespace = "test_";
    static String testName = "table_1";
    static UUID workspaceId = UUID.randomUUID();
    static UUID connectionId = UUID.randomUUID();
    static Long jobId = ThreadLocalRandom.current().nextLong();
    static Long transitionedAtMs = System.currentTimeMillis();

    static StreamStatusCreateRequestBody validCreate() {
      return new StreamStatusCreateRequestBody()
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .jobType(StreamStatusJobType.SYNC)
          .attemptNumber(0)
          .streamNamespace(testNamespace)
          .streamName(testName)
          .runState(StreamStatusRunState.PENDING)
          .transitionedAt(transitionedAtMs);
    }

    static StreamStatusUpdateRequestBody validUpdate() {
      return new StreamStatusUpdateRequestBody()
          .workspaceId(workspaceId)
          .connectionId(connectionId)
          .jobId(jobId)
          .jobType(StreamStatusJobType.SYNC)
          .attemptNumber(0)
          .streamNamespace(testNamespace)
          .streamName(testName)
          .runState(StreamStatusRunState.PENDING)
          .transitionedAt(transitionedAtMs)
          .id(UUID.randomUUID());
    }

    static Pagination validPagination() {
      return new Pagination()
          .pageSize(10)
          .rowOffset(0);
    }

    static StreamStatusListRequestBody validList() {
      return new StreamStatusListRequestBody()
          .workspaceId(UUID.randomUUID())
          .jobId(ThreadLocalRandom.current().nextLong())
          .pagination(validPagination());
    }

  }

}
