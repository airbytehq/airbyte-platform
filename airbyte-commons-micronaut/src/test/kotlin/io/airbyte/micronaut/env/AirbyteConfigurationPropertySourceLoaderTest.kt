/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.PropertySource
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.core.io.ResourceLoader
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.stream.Stream

internal class AirbyteConfigurationPropertySourceLoaderTest {
  private val resourceLoader = mockk<ResourceLoader>(relaxed = true)
  private lateinit var loader: AirbyteConfigurationPropertySourceLoader

  @BeforeEach
  fun setup() {
    loader = AirbyteConfigurationPropertySourceLoader()
  }

  @Test
  fun testLoadingAirbyteConfigurations() {
    val configFiles = listOf(javaClass.getResource("/test-airbyte-configuration.yml"))
    every { resourceLoader.getResources(any()) } returns configFiles.stream()

    val propertySource = loader.load("test", resourceLoader)
    assertTrue(propertySource.isPresent)
    assertEquals(5, propertySource.get().get("airbyte.test.value"))
  }

  @Test
  fun testLoadingNoAirbyteConfigurations() {
    every { resourceLoader.getResources(any()) } returns Stream.of()

    val propertySource = loader.load("test", resourceLoader)
    assertTrue(propertySource.isPresent)
    assertEquals(0, propertySource.get().count())
  }

  @Test
  fun testLoadingAirbyteConfigurationsWithException() {
    every { resourceLoader.getResources(any()) } throws IOException("test")

    val e =
      assertThrows(ConfigurationException::class.java) {
        loader.load("test", resourceLoader)
      }
    assertEquals(IOException::class.java, e.cause?.javaClass)
  }

  @Test
  fun testPropertySourceDetails() {
    val configFiles = listOf(javaClass.getResource("/test-airbyte-configuration.yml"))
    every { resourceLoader.getResources(any()) } returns configFiles.stream()

    val propertySource = loader.load("test", resourceLoader)
    assertTrue(propertySource.isPresent)
    assertEquals(NAME, propertySource.get().name)
    assertEquals("$NAME.yml", propertySource.get().origin.location())
    assertEquals(DEFAULT_POSITION, propertySource.get().order)
    assertEquals(PropertySource.PropertyConvention.JAVA_PROPERTIES, propertySource.get().convention)
  }
}
