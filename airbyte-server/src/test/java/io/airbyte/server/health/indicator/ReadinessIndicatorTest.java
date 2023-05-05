/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.health.indicator;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests the readiness indicator probe endpoint.
 */
@MicronautTest
@Requires(property = "mockito.test.enabled",
          defaultValue = StringUtils.TRUE,
          value = StringUtils.TRUE)
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class ReadinessIndicatorTest extends BaseIndicatorTest {

  final String path = "/health/readiness";

  @Test
  void testReadinessEndpointSuccess() {
    Mockito.when(healthCheckHandler.isReady()).thenReturn(true);
    testEndpointStatus(HttpRequest.GET(path), HttpStatus.OK);
  }

  @Test
  void testReadinessEndpointFailure() {
    Mockito.when(healthCheckHandler.isReady()).thenReturn(false);
    testErrorEndpointStatus(HttpRequest.GET(path), HttpStatus.SERVICE_UNAVAILABLE);
  }

}
