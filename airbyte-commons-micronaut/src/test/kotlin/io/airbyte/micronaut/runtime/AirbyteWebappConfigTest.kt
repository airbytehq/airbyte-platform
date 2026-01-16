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
internal class AirbyteWebappConfigDefaultTest {
  @Inject
  private lateinit var arbyteWebappConfig: AirbyteWebappConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", arbyteWebappConfig.datadogApplicationId)
    assertEquals("", arbyteWebappConfig.datadogClientToken)
    assertEquals("", arbyteWebappConfig.datadogEnv)
    assertEquals("", arbyteWebappConfig.datadogService)
    assertEquals("", arbyteWebappConfig.datadogSite)
    assertEquals("", arbyteWebappConfig.hockeystackApiKey)
    assertEquals("", arbyteWebappConfig.launchdarklyKey)
    assertEquals("", arbyteWebappConfig.osanoKey)
    assertEquals("", arbyteWebappConfig.posthogApiKey)
    assertEquals("", arbyteWebappConfig.posthogHost)
    assertEquals("", arbyteWebappConfig.segmentToken)
    assertEquals("", arbyteWebappConfig.sonarApiUrl)
    assertEquals("", arbyteWebappConfig.url)
    assertEquals("", arbyteWebappConfig.zendeskKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-webapp.yml"])
internal class AirbyteWebappConfigOverridesTest {
  @Inject
  private lateinit var arbyteWebappConfig: AirbyteWebappConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-datadog-application-id", arbyteWebappConfig.datadogApplicationId)
    assertEquals("test-datadog-client-token", arbyteWebappConfig.datadogClientToken)
    assertEquals("test-datadog-env", arbyteWebappConfig.datadogEnv)
    assertEquals("test-datadog-service", arbyteWebappConfig.datadogService)
    assertEquals("test-datadog-site", arbyteWebappConfig.datadogSite)
    assertEquals("test-hockeystack-api-key", arbyteWebappConfig.hockeystackApiKey)
    assertEquals("test-launchdarkly-key", arbyteWebappConfig.launchdarklyKey)
    assertEquals("test-osano-key", arbyteWebappConfig.osanoKey)
    assertEquals("test-posthog-api-key", arbyteWebappConfig.posthogApiKey)
    assertEquals("test-posthog-host", arbyteWebappConfig.posthogHost)
    assertEquals("test-segment-token", arbyteWebappConfig.segmentToken)
    assertEquals("test-sonar-api-url", arbyteWebappConfig.sonarApiUrl)
    assertEquals("test-url", arbyteWebappConfig.url)
    assertEquals("test-zendesk-key", arbyteWebappConfig.zendeskKey)
  }
}
