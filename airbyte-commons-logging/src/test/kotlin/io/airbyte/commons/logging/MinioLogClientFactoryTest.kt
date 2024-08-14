/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class MinioLogClientFactoryTest {
  @Test
  internal fun testClientCreation() {
    val accessKey = "accessKey"
    val secretAccessKey = "secretAccessKey"
    val endpoint = "http://airbyte-minio-svc:9000"
    val factory = MinioS3LogClientFactory(accessKey = accessKey, secretAccessKey = secretAccessKey, endpoint = endpoint)
    val client = factory.get()
    assertNotNull(client)
  }

  @Test
  internal fun testFailedClientCreation() {
    val throwable =
      assertThrows<RuntimeException> {
        val accessKey = "accessKey"
        val secretAccessKey = "secretAccessKey"
        val factory = MinioS3LogClientFactory(accessKey = accessKey, secretAccessKey = secretAccessKey, endpoint = "notvalid")
        factory.get()
      }
    assertEquals(NullPointerException::class.java, throwable.cause?.javaClass)
  }
}
