/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionStream;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.temporal.client.WorkflowClient;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Micronaut-based test suite for the {@link ConnectionApiController} class.
 */
@MicronautTest(environments = Environment.TEST)
@Requires(property = "mockito.test.enabled",
          defaultValue = StringUtils.FALSE,
          value = StringUtils.TRUE)
class ConnectionApiControllerTest {

  @Inject
  ConnectionApiController connectionApiController;

  @Inject
  SchedulerHandler schedulerHandler;

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
    assertEquals(expectedJobInfoRead, jobInfoRead);
  }

  @MockBean(SchedulerHandler.class)
  SchedulerHandler schedulerHandler() {
    return mock(SchedulerHandler.class);
  }

  @MockBean(WorkflowClient.class)
  WorkflowClient workflowClient() {
    return mock(WorkflowClient.class);
  }

}
