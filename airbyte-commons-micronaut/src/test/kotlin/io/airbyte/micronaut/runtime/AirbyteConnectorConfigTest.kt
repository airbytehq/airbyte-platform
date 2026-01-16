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
internal class AirbyteConnectorConfigDefaultTest {
  @Inject
  private lateinit var airbyteConnectorConfig: AirbyteConnectorConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_CONNECTOR_CONFIG_DIR, airbyteConnectorConfig.configDir)
    assertEquals(false, airbyteConnectorConfig.specificResourceDefaultsEnabled)
    assertEquals(DEFAULT_AWS_ASSUMED_ROLE_ACCESS_KEY, airbyteConnectorConfig.source.credentials.aws.assumedRole.accessKey)
    assertEquals(DEFAULT_AWS_ASSUMED_ROLE_SECRET_KEY, airbyteConnectorConfig.source.credentials.aws.assumedRole.secretKey)
    assertEquals("", airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName)
    assertEquals("", airbyteConnectorConfig.stagingDir)
  }
}

@MicronautTest(propertySources = ["classpath:application-connector.yml"])
internal class AirbyteConnectorConfigOverridesTest {
  @Inject
  private lateinit var airbyteConnectorConfig: AirbyteConnectorConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("/test-config-dir", airbyteConnectorConfig.configDir)
    assertEquals(true, airbyteConnectorConfig.specificResourceDefaultsEnabled)
    assertEquals("test-access-key", airbyteConnectorConfig.source.credentials.aws.assumedRole.accessKey)
    assertEquals("test-secret-key", airbyteConnectorConfig.source.credentials.aws.assumedRole.secretKey)
    assertEquals("test-secret-name", airbyteConnectorConfig.source.credentials.aws.assumedRole.secretName)
    assertEquals("/staging-dir", airbyteConnectorConfig.stagingDir)
  }
}
