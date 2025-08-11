/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.core.io.ResourceLoader
import io.mockk.Called
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class EditionPropertySourceLoaderTest {
  private val resourceLoader = mockk<ResourceLoader>(relaxed = true)

  @Test
  fun `should return empty optional when AIRBYTE_EDITION is not set`() {
    val loader = EditionPropertySourceLoader(null)

    val result = loader.load("test", resourceLoader)

    Assertions.assertTrue(result.isEmpty)
    verify { resourceLoader wasNot Called }
  }

  @Test
  fun `should return empty optional when AIRBYTE_EDITION is empty`() {
    val loader = EditionPropertySourceLoader("")

    val result = loader.load("test", resourceLoader)

    Assertions.assertTrue(result.isEmpty)
    verify { resourceLoader wasNot Called }
  }

  @Test
  fun `should attempt to load correct file when AIRBYTE_EDITION is set`() {
    val loader = EditionPropertySourceLoader("community")

    loader.load("test", resourceLoader)

    // ensures underlying loader is called with correct file name and extensions
    verify { resourceLoader.getResourceAsStream("application-edition-community.yml") }
    verify { resourceLoader.getResourceAsStream("application-edition-community.yaml") }
  }
}
