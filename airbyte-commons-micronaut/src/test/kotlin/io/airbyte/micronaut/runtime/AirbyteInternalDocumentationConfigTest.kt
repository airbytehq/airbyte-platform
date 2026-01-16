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
internal class AirbyteInternalDocumentationConfigDefaultTest {
  @Inject
  private lateinit var airbyteInternalDocumentationConfig: AirbyteInternalDocumentationConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_INTERNAL_DOCUMENTATION_HOST, airbyteInternalDocumentationConfig.host)
  }
}

@MicronautTest(propertySources = ["classpath:application-internal-documentation.yml"])
internal class AirbyteInternalDocumentationConfigOverridesTest {
  @Inject
  private lateinit var airbyteInternalDocumentationConfig: AirbyteInternalDocumentationConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-host", airbyteInternalDocumentationConfig.host)
  }
}
