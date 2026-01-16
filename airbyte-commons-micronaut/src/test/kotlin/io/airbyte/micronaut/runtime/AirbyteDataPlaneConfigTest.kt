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
internal class AirbyteDataPlaneConfigDefaultTest {
  @Inject
  private lateinit var airbyteDataPlaneConfig: AirbyteDataPlaneConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteDataPlaneConfig.credentials.clientIdSecretKey)
    assertEquals("", airbyteDataPlaneConfig.credentials.clientIdSecretName)
    assertEquals("", airbyteDataPlaneConfig.credentials.clientSecretSecretKey)
    assertEquals("", airbyteDataPlaneConfig.credentials.clientSecretSecretName)
    assertEquals("", airbyteDataPlaneConfig.serviceAccount.credentialsPath)
    assertEquals("", airbyteDataPlaneConfig.serviceAccount.email)
  }
}

@MicronautTest(propertySources = ["classpath:application-data-plane.yml"])
internal class AirbyteDataPlanerConfigOverridesTest {
  @Inject
  private lateinit var airbyteDataPlaneConfig: AirbyteDataPlaneConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-client-id-secret-key", airbyteDataPlaneConfig.credentials.clientIdSecretKey)
    assertEquals("test-client-id-secret-name", airbyteDataPlaneConfig.credentials.clientIdSecretName)
    assertEquals("test-client-secret-secret-key", airbyteDataPlaneConfig.credentials.clientSecretSecretKey)
    assertEquals("test-client-secret-secret-name", airbyteDataPlaneConfig.credentials.clientSecretSecretName)
    assertEquals("test-credentials-path", airbyteDataPlaneConfig.serviceAccount.credentialsPath)
    assertEquals("test@airbyte.io", airbyteDataPlaneConfig.serviceAccount.email)
  }
}
