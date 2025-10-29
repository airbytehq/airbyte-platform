/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.runtime

import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
@Property(name = "micronaut.server.port", value = "-1")
internal class AirbyteConnectorRolloutConfigurationDefaultTest {
  @Inject
  private lateinit var airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig

  @Test
  fun testLoadingValuesFromConfiguration() {
    assertEquals(DEFAULT_CONNECTOR_GITHUB_ROLLOUT_DISPATCH_URL, airbyteConnectorRolloutConfig.githubRollout.dispatchUrl)
    assertEquals("", airbyteConnectorRolloutConfig.githubRollout.githubToken)
  }
}

@MicronautTest(propertySources = ["classpath:application-connector-rollout.yml"])
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
@Property(name = "micronaut.server.port", value = "-1")
internal class AirbyteControlPlanerConfigurationOverridesTest {
  @Inject
  private lateinit var airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig

  @Test
  fun testLoadingValuesFromConfiguration() {
    assertEquals("http://test-dispatch-url", airbyteConnectorRolloutConfig.githubRollout.dispatchUrl)
    assertEquals("test-token", airbyteConnectorRolloutConfig.githubRollout.githubToken)
  }
}
