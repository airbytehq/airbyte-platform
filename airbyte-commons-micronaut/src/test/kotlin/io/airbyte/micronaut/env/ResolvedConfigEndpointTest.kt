/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.Environment
import io.micronaut.context.env.PropertySource
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class ResolvedConfigEndpointTest {
  @Test
  fun testResolvedConfig() {
    val name = "default"
    val key = "airbyte.test"
    val value = "a really long string"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val environment =
      mockk<Environment> {
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(maskValue(value), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigWithOverrides() {
    val name1 = "default"
    val name2 = "default2"
    val key = "airbyte.test"
    val value1 = "a really long string"
    val value2 = "a really long string with more details in it"
    val config1 = mapOf(key to value1)
    val config2 = mapOf(key to value2)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource1 = PropertySource.of(name1, config1, origin, -100)
    val propertySource2 = PropertySource.of(name2, config2, origin, 100)
    val environment =
      mockk<Environment> {
        every { propertySources } returns setOf(propertySource1, propertySource2)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(maskValue(value2), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigWithShortValue() {
    val name = "default"
    val key = "airbyte.test"
    val value = "abcd"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val environment =
      mockk<Environment> {
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(maskValue(value), resolvedConfiguration.values.first().value)
    assertTrue(
      resolvedConfiguration.values
        .first()
        .value
        .toString()
        .all { it == '*' },
    )
  }
}
