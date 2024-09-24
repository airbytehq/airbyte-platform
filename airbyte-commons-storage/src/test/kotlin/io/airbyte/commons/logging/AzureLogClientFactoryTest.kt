/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class AzureLogClientFactoryTest {
  @Test
  internal fun testClientCreation() {
    val connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test"
    val factory = AzureLogClientFactory(connectionString)
    val client = factory.get()
    assertNotNull(client)
  }

  @Test
  internal fun testFailedClientCreation() {
    val throwable =
      assertThrows<RuntimeException> {
        val connectionString = "invalid string"
        val factory = AzureLogClientFactory(connectionString)
        factory.get()
      }
    assertEquals(IllegalArgumentException::class.java, throwable.cause?.javaClass)
  }
}
