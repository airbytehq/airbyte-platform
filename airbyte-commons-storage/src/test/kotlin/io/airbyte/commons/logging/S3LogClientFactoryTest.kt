/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.regions.Region

internal class S3LogClientFactoryTest {
  @Test
  internal fun testClientCreation() {
    val accessKey = "accessKey"
    val secretAccessKey = "secretAccessKey"
    val region = Region.US_EAST_1.toString()
    val factory = S3LogClientFactory(accessKey = accessKey, secretAccessKey = secretAccessKey, region = region)
    val client = factory.get()
    assertNotNull(client)
  }

  @Test
  internal fun testFailedClientCreation() {
    val throwable =
      assertThrows<RuntimeException> {
        val accessKey = "accessKey"
        val secretAccessKey = "secretAccessKey"
        val region = ""
        val factory = S3LogClientFactory(accessKey = accessKey, secretAccessKey = secretAccessKey, region = region)
        factory.get()
      }
    assertEquals(IllegalArgumentException::class.java, throwable.cause?.javaClass)
  }
}
