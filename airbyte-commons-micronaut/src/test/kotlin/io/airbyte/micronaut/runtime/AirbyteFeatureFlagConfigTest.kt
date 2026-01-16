/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteFeatureFlagConfigDefaultTest {
  @Inject
  private lateinit var airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteFeatureFlagConfig.apiKey)
    assertEquals("", airbyteFeatureFlagConfig.baseUrl)
    assertEquals(AirbyteFeatureFlagConfig.FeatureFlagClientType.CONFIGFILE, airbyteFeatureFlagConfig.client)
    assertEquals(Path.of(DEFAULT_FEATURE_FLAG_PATH), airbyteFeatureFlagConfig.path)
  }
}

@MicronautTest(propertySources = ["classpath:application-feature-flag-ffs.yml"])
internal class AirbyteFeatureFlagConfigFeatureFlagServiceTest {
  @Inject
  private lateinit var airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteFeatureFlagConfig.apiKey)
    assertEquals("http://localhost:8080", airbyteFeatureFlagConfig.baseUrl)
    assertEquals(AirbyteFeatureFlagConfig.FeatureFlagClientType.FFS, airbyteFeatureFlagConfig.client)
    assertEquals(Path.of(DEFAULT_FEATURE_FLAG_PATH), airbyteFeatureFlagConfig.path)
  }
}

@MicronautTest(propertySources = ["classpath:application-feature-flag-launchdarkly.yml"])
internal class AirbyteFeatureFlagConfigLaunchdarklyTest {
  @Inject
  private lateinit var airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-api-key", airbyteFeatureFlagConfig.apiKey)
    assertEquals("", airbyteFeatureFlagConfig.baseUrl)
    assertEquals(AirbyteFeatureFlagConfig.FeatureFlagClientType.LAUNCHDARKLY, airbyteFeatureFlagConfig.client)
    assertEquals(Path.of(DEFAULT_FEATURE_FLAG_PATH), airbyteFeatureFlagConfig.path)
  }
}

@MicronautTest(propertySources = ["classpath:application-feature-flag-config.yml"])
internal class AirbyteFeatureFlagConfigConfigTest {
  @Inject
  private lateinit var airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteFeatureFlagConfig.apiKey)
    assertEquals("", airbyteFeatureFlagConfig.baseUrl)
    assertEquals(AirbyteFeatureFlagConfig.FeatureFlagClientType.CONFIGFILE, airbyteFeatureFlagConfig.client)
    assertEquals(Path.of("/some/path"), airbyteFeatureFlagConfig.path)
  }
}
