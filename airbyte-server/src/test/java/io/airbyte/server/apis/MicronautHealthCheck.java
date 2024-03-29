/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.HealthCheckRead;
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
class MicronautHealthCheck extends BaseControllerTest {

  @Test
  void testHealth() {
    Mockito.when(healthCheckHandler.health())
        .thenReturn(new HealthCheckRead());
    testEndpointStatus(
        HttpRequest.GET("/api/v1/health"), HttpStatus.OK);
  }

}
