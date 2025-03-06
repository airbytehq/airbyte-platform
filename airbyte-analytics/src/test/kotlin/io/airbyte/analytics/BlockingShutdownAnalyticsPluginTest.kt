/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import com.segment.analytics.Analytics
import com.segment.analytics.messages.TrackMessage
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import retrofit.client.Client
import retrofit.client.Response
import retrofit.mime.TypedInput
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class BlockingShutdownAnalyticsPluginTest {
  @Test
  fun `test that the plugin handles the timeout waiting on the client to flush messages`() {
    val flushInterval = 2L
    val blockingShutdownAnalyticsPlugin = BlockingShutdownAnalyticsPlugin(flushInterval)

    assertDoesNotThrow {
      blockingShutdownAnalyticsPlugin.waitForFlush()
    }
  }

  @Test
  fun `test that all in-flight messages are flushed before shutdown`() {
    val body: TypedInput = mockk()
    val bodyJson = "{}"
    val client: Client = mockk()
    val response: Response = mockk()
    val flushInterval = 3L
    val writeKey = "write-key"
    val plugin = BlockingShutdownAnalyticsPlugin(flushInterval)

    every { body.`in`() } returns bodyJson.byteInputStream()
    every { body.length() } returns bodyJson.toByteArray().size.toLong()
    every { body.mimeType() } returns "application/json"
    every { response.body } returns body
    every { response.headers } returns listOf()
    every { response.reason } returns "Reason"
    every { response.status } returns HttpStatus.OK.code
    every { response.url } returns "http://localhost"
    every { client.execute(any()) } returns response

    val analytics =
      Analytics
        .builder(writeKey)
        .client(client)
        .flushInterval(flushInterval, TimeUnit.SECONDS)
        .flushQueueSize(5001)
        .plugin(plugin)
        .build()

    assertDoesNotThrow {
      CompletableFuture.supplyAsync {
        for (i in 0..5000) {
          val builder = TrackMessage.builder("track").userId("user-id").properties(mapOf("property" to "value"))
          analytics.enqueue(builder)
        }
      }
      plugin.waitForFlush()
    }
    assertEquals(0, plugin.currentInflightMessageCount())
  }
}
