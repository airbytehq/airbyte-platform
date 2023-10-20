/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionStream;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.GetTaskQueueNameRequest;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.scheduling.RouterService;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.micronaut.context.env.Environment;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.temporal.client.WorkflowClient;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Micronaut-based test suite for the {@link ConnectionApiController} class.
 */
@MicronautTest(environments = Environment.TEST)
class ConnectionApiControllerTest {

  @Inject
  ConnectionApiController connectionApiController;

  @Inject
  SchedulerHandler schedulerHandler;

  @Inject
  RouterService routerService;

  @Test
  void testConnectionStreamReset() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String streamName = "tableA";
    final String streamNamespace = "schemaA";
    final ConnectionStream connectionStream = new ConnectionStream()
        .streamName(streamName)
        .streamNamespace(streamNamespace);
    final ConnectionStreamRequestBody connectionStreamRequestBody = new ConnectionStreamRequestBody()
        .connectionId(connectionId)
        .streams(List.of(connectionStream));
    final JobInfoRead expectedJobInfoRead = new JobInfoRead();

    when(schedulerHandler.resetConnectionStream(connectionStreamRequestBody)).thenReturn(expectedJobInfoRead);

    final JobInfoRead jobInfoRead = connectionApiController.resetConnectionStream(connectionStreamRequestBody);
    Assertions.assertEquals(expectedJobInfoRead, jobInfoRead);
  }

  @ParameterizedTest
  @MethodSource("uuidJobTypesMatrix")
  void getTaskQueueNameSuccess(final UUID connectionId, final TemporalJobType jobType) throws IOException, ConfigNotFoundException {
    final var expected = "queue name";
    when(routerService.getTaskQueue(connectionId, jobType)).thenReturn(expected);

    final var result = connectionApiController.getTaskQueueName(
        new GetTaskQueueNameRequest()
            .connectionId(connectionId)
            .temporalJobType(jobType.toString()));

    Assertions.assertEquals(expected, result.getTaskQueueName());
  }

  @Test
  void getTaskQueueNameNotFound() throws IOException, ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();
    final TemporalJobType type = TemporalJobType.SYNC;

    when(routerService.getTaskQueue(connectionId, type))
        .thenThrow(new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC, "Nope."));

    final var req = new GetTaskQueueNameRequest()
        .connectionId(connectionId)
        .temporalJobType(type.toString());

    Assertions.assertThrows(IdNotFoundKnownException.class, () -> connectionApiController.getTaskQueueName(req));
  }

  @Test
  void getTaskQueueNameCannotParseJobType() {
    final UUID connectionId = UUID.randomUUID();
    final String type = TemporalJobType.SYNC + "bang";

    final var req = new GetTaskQueueNameRequest()
        .connectionId(connectionId)
        .temporalJobType(type);

    Assertions.assertThrows(BadRequestException.class, () -> connectionApiController.getTaskQueueName(req));
  }

  static Stream<Arguments> uuidJobTypesMatrix() {
    return Arrays.stream(TemporalJobType.values()).map(v -> Arguments.of(UUID.randomUUID(), v));
  }

  @MockBean(SchedulerHandler.class)
  SchedulerHandler schedulerHandler() {
    return mock(SchedulerHandler.class);
  }

  @MockBean(WorkflowClient.class)
  WorkflowClient workflowClient() {
    return mock(WorkflowClient.class);
  }

  @MockBean(RouterService.class)
  RouterService routerService() {
    return mock(RouterService.class);
  }

}
