/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteControlPlaneConfigDefaultTest {
  @Inject
  private lateinit var airbyteControlPlaneConfig: AirbyteControlPlaneConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteControlPlaneConfig.authEndpoint)
  }
}

@MicronautTest(propertySources = ["classpath:application-control-plane.yml"])
internal class AirbyteControlPlanerConfigOverridesTest {
  @Inject
  private lateinit var airbyteControlPlaneConfig: AirbyteControlPlaneConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://test-auth-endpoint", airbyteControlPlaneConfig.authEndpoint)
  }
}
