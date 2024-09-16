/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.NoSuchFileException

internal class GcsLogClientFactoryTest {
  @Test
  internal fun testClientCreation() {
    val applicationCredentials = MoreResources.readResourceAsFile("sample_gcs_credentials.json")
    val factory = GcsLogClientFactory(applicationCredentials = applicationCredentials.path)
    val client = factory.get()
    assertNotNull(client)
  }

  @Test
  internal fun testFailedClientCreation() {
    val throwable =
      assertThrows<RuntimeException> {
        val factory = GcsLogClientFactory(applicationCredentials = "doesnotexist")
        factory.get()
      }
    assertEquals(NoSuchFileException::class.java, throwable.cause?.javaClass)
  }
}
