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
internal class AirbyteSupportEmailDomainsConfigDefaultTest {
  @Inject
  private lateinit var airbyteSupportEmailDomainsConfig: AirbyteSupportEmailDomainsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteSupportEmailDomainsConfig.oss)
    assertEquals(DEFAULT_SUPPORT_CLOUD_EMAIL_DOMAIN, airbyteSupportEmailDomainsConfig.cloud)
  }
}

@MicronautTest(propertySources = ["classpath:application-support.yml"])
internal class AirbyteSupportEmailDomainsConfigOverridesTest {
  @Inject
  private lateinit var airbyteSupportEmailDomainsConfig: AirbyteSupportEmailDomainsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-oss-domain", airbyteSupportEmailDomainsConfig.oss)
    assertEquals("test-cloud-domain", airbyteSupportEmailDomainsConfig.cloud)
  }
}
