/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.runtime

import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
internal class AirbyteServerConfigurationDefaultTest {
  @Inject
  private lateinit var airbyteServerConfiguration: AirbyteServerConfiguration

  @Test
  fun testLoadingValuesFromConfiguration() {
    assertEquals(DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS, airbyteServerConfiguration.connectionLimits.limits.maxDays)
    assertEquals(DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS_WARNING, airbyteServerConfiguration.connectionLimits.limits.maxDaysWarning)
    assertEquals(DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS, airbyteServerConfiguration.connectionLimits.limits.maxJobs)
    assertEquals(DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS_WARNING, airbyteServerConfiguration.connectionLimits.limits.maxJobsWarning)
    assertEquals(DEFAULT_SERVER_CONNECTION_LIMIT_MAX_FIELDS_PER_CONNECTION, airbyteServerConfiguration.connectionLimits.limits.maxFieldsPerConnection)
    assertEquals(DEFAULT_SERVER_LIMIT_CONNECTIONS, airbyteServerConfiguration.limits.connections)
    assertEquals(DEFAULT_SERVER_LIMIT_DESTINATIONS, airbyteServerConfiguration.limits.destinations)
    assertEquals(DEFAULT_SERVER_LIMIT_SOURCES, airbyteServerConfiguration.limits.sources)
    assertEquals(DEFAULT_SERVER_LIMIT_USERS, airbyteServerConfiguration.limits.users)
    assertEquals(DEFAULT_SERVER_LIMIT_WORKSPACES, airbyteServerConfiguration.limits.workspaces)
  }
}

@MicronautTest(propertySources = ["classpath:application-server.yml"])
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
internal class AirbyteServerConfigurationOverridesTest {
  @Inject
  private lateinit var airbyteServerConfiguration: AirbyteServerConfiguration

  @Test
  fun testLoadingValuesFromConfiguration() {
    assertEquals(10L, airbyteServerConfiguration.connectionLimits.limits.maxDays)
    assertEquals(6L, airbyteServerConfiguration.connectionLimits.limits.maxDaysWarning)
    assertEquals(12L, airbyteServerConfiguration.connectionLimits.limits.maxJobs)
    assertEquals(11L, airbyteServerConfiguration.connectionLimits.limits.maxJobsWarning)
    assertEquals(13L, airbyteServerConfiguration.connectionLimits.limits.maxFieldsPerConnection)
    assertEquals(1L, airbyteServerConfiguration.limits.connections)
    assertEquals(3L, airbyteServerConfiguration.limits.destinations)
    assertEquals(2L, airbyteServerConfiguration.limits.sources)
    assertEquals(5L, airbyteServerConfiguration.limits.users)
    assertEquals(4L, airbyteServerConfiguration.limits.workspaces)
  }
}
