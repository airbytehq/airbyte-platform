/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.health.indicator;

import static org.junit.jupiter.api.Assertions.*;

import io.airbyte.commons.server.handlers.HealthCheckHandler;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.mockito.Mockito;

/**
 * Abstract test class for health indicators.
 */
@MicronautTest
@Requires(property = "mockito.test.enabled",
          defaultValue = StringUtils.TRUE,
          value = StringUtils.TRUE)
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
abstract class BaseIndicatorTest {

  HealthCheckHandler healthCheckHandler = Mockito.mock(HealthCheckHandler.class);

  @MockBean(HealthCheckHandler.class)
  @Replaces(HealthCheckHandler.class)
  HealthCheckHandler mmHealthCheckHandler() {
    return healthCheckHandler;
  }

  @Inject
  @Client("/")
  HttpClient client;

  void testEndpointStatus(final HttpRequest request, final HttpStatus expectedStatus) {
    assertEquals(expectedStatus, client.toBlocking().exchange(request).getStatus());
  }

  void testErrorEndpointStatus(final HttpRequest request, final HttpStatus expectedStatus) {
    Assertions.assertThatThrownBy(() -> client.toBlocking().exchange(request))
        .isInstanceOf(HttpClientResponseException.class)
        .asInstanceOf(new InstanceOfAssertFactory(HttpClientResponseException.class, Assertions::assertThat))
        .has(new Condition<HttpClientResponseException>(exception -> exception.getStatus() == expectedStatus,
            "Http status to be %s", expectedStatus));
  }

}
