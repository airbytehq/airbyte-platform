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
internal class AirbyteDataplaneGroupsConfigDefaultTest {
  @Inject
  private lateinit var airbyteDataplaneGroupsConfig: AirbyteDataplaneGroupsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("AUTO", airbyteDataplaneGroupsConfig.defaultDataplaneGroupName)
  }
}

@MicronautTest(propertySources = ["classpath:application-dataplane-groups.yml"])
internal class AirbyteDataplaneGroupsConfigOverridesTest {
  @Inject
  private lateinit var airbyteDataplaneGroupsConfig: AirbyteDataplaneGroupsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("TEST_DEFAULT_GROUP", airbyteDataplaneGroupsConfig.defaultDataplaneGroupName)
  }
}
