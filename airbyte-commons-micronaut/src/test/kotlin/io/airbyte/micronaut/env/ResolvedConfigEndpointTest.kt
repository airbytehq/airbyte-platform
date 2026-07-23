/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.Environment
import io.micronaut.context.env.PropertyPlaceholderResolver
import io.micronaut.context.env.PropertySource
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Optional

internal class ResolvedConfigEndpointTest {
  @Test
  fun testResolvedConfig() {
    val name = "default"
    val key = "airbyte.test"
    val value = "a really long string"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(endpoint.maskValue(value, false), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigAlwaysMask() {
    val name = "default"
    val key = "airbyte.password"
    val value = "a really long string"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(endpoint.maskValue(value, true), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigWithPlaceholder() {
    val name = "default"
    val key = "airbyte.test"
    val value = "\${PROPERTY_PLACEHOLDER:default}"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.of("a really long string")
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(endpoint.maskValue(value, false), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigWithPlaceholderNoDefault() {
    val name = "default"
    val key = "airbyte.test"
    val value = "\${PROPERTY_PLACEHOLDER}"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(MISSING_DEFAULT_VALUE, resolvedConfiguration.values.first().value)
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
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource1, propertySource2)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(endpoint.maskValue(value2, false), resolvedConfiguration.values.first().value)
  }

  @Test
  fun testResolvedConfigWithShortValue() {
    val name = "default"
    val key = "airbyte.test"
    val value = "abcd"
    val config = mapOf(key to value)
    val origin = PropertySource.Origin.of("someFile")
    val propertySource = PropertySource.of(name, config, origin)
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns setOf(propertySource)
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfiguration()
    assertEquals(1, resolvedConfiguration.keys.size)
    assertEquals(key, resolvedConfiguration.keys.first())
    assertEquals(origin.location(), resolvedConfiguration.values.first().location)
    assertEquals(endpoint.maskValue(value, false), resolvedConfiguration.values.first().value)
    assertTrue(
      resolvedConfiguration.values
        .first()
        .value
        .toString()
        .all { it == '*' },
    )
  }

  @Test
  fun testResolvedConfigProperty() {
    val propertyName = "property-name"
    val lowValue = "low-value"
    val mediumValue = "medium-value"
    val highValue = "high-value"
    val lowConfig = mapOf(propertyName to lowValue)
    val mediumConfig = mapOf(propertyName to mediumValue)
    val highConfig = mapOf(propertyName to highValue)
    val propertySourceSet =
      setOf(
        PropertySource.of("low", lowConfig, PropertySource.Origin.of("low"), -100),
        PropertySource.of("medium", mediumConfig, PropertySource.Origin.of("medium"), 0),
        PropertySource.of("high", highConfig, PropertySource.Origin.of("high"), 100),
      )
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
        every { propertySources } returns propertySourceSet
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val resolvedConfiguration = endpoint.getResolvedConfigurationProperty(property = propertyName)
    assertEquals(endpoint.maskValue(highValue, false), resolvedConfiguration.first().details.value)
    assertEquals(endpoint.maskValue(lowValue, false), resolvedConfiguration.last().details.value)
  }

  @Test
  fun testMaskingNullValue() {
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(null, false)
    assertEquals("null", result)
  }

  @Test
  fun testMaskingEmptyValue() {
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue("", false)
    assertEquals("", result)
  }

  @Test
  fun testMaskingIntegerValue() {
    val value = 5
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals(value.toString(), result)
  }

  @Test
  fun testMaskingDecimalValue() {
    val value = 5.4
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals(value.toString(), result)
  }

  @Test
  fun testMaskingStringValue() {
    val value = "a very long string that needs masking"
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertTrue(result.startsWith(MASK_VALUE))
    assertEquals(value.takeLast(UNMASKED_LENGTH), result.replace(MASK_VALUE, ""))
  }

  @Test
  fun testMaskingArrayValue() {
    val value = arrayOf("a string one", "a string two", "a string three")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one,$MASK_VALUE two,${MASK_VALUE}hree", result)
  }

  @Test
  fun testMaskingArraySingleValue() {
    val value = arrayOf("a string one")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one", result)
  }

  @Test
  fun testMaskingArrayValueWithEmptyString() {
    val value = arrayOf("a string one", "", "a string three")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one,${MASK_VALUE}hree", result)
  }

  @Test
  fun testMaskingArrayValueWithAllEmptyStrings() {
    val value = arrayOf("", "", "")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("", result)
  }

  @Test
  fun testMaskingListValue() {
    val value = listOf("a string one", "a string two", "a string three")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one,$MASK_VALUE two,${MASK_VALUE}hree", result)
  }

  @Test
  fun testMaskingListSingleValue() {
    val value = listOf("a string one")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one", result)
  }

  @Test
  fun testMaskingListValueWithEmptyString() {
    val value = listOf("a string one", "", "a string three")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("$MASK_VALUE one,${MASK_VALUE}hree", result)
  }

  @Test
  fun testMaskingListValueWithAllEmptyStrings() {
    val value = listOf("", "", "")
    val propertyPlaceholderResolver =
      mockk<PropertyPlaceholderResolver> {
        every { resolvePlaceholders(any()) } returns Optional.empty()
      }
    val environment =
      mockk<Environment> {
        every { placeholderResolver } returns propertyPlaceholderResolver
      }
    val endpoint = ResolvedConfigEndpoint(environment)
    val result = endpoint.maskValue(value, false)
    assertEquals("", result)
  }
}
