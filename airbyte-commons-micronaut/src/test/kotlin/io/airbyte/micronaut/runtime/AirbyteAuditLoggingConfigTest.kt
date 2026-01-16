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
internal class AirbyteAuditLoggingConfigDefaultTest {
  @Inject
  private lateinit var airbyteAuditLoggingConfig: AirbyteAuditLoggingConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteAuditLoggingConfig.enabled)
  }
}

@MicronautTest(propertySources = ["classpath:application-audit-logging.yml"])
internal class AirbyteAuditLoggingConfigStiggTest {
  @Inject
  private lateinit var airbyteAuditLoggingConfig: AirbyteAuditLoggingConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteAuditLoggingConfig.enabled)
  }
}
