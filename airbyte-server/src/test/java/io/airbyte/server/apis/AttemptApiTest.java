/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.SaveStatsRequestBody;
import io.airbyte.api.model.generated.SetWorkflowInAttemptRequestBody;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class AttemptApiTest extends BaseControllerTest {

  @Test
  void testSaveState() {
    Mockito.when(attemptHandler.saveStats(Mockito.any()))
        .thenReturn(new InternalOperationResult());
    final String path = "/api/v1/attempt/save_stats";
    testEndpointStatus(
        HttpRequest.POST(path, new SaveStatsRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testSetWorkflowInAttempt() {
    Mockito.when(attemptHandler.setWorkflowInAttempt(Mockito.any()))
        .thenReturn(new InternalOperationResult());
    final String path = "/api/v1/attempt/set_workflow_in_attempt";
    testEndpointStatus(
        HttpRequest.POST(path, new SetWorkflowInAttemptRequestBody()),
        HttpStatus.OK);
  }

}
