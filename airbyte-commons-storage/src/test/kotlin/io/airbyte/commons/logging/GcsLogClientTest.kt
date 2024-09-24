/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.storage.StorageBucketConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.OutputStream

internal class GcsLogClientTest {
  @Test
  internal fun testLogDeletion() {
    val blob =
      mockk<Blob> {
        every { delete(any()) } returns true
      }
    val page =
      mockk<Page<Blob>> {
        every { iterateAll() } returns listOf(blob)
      }
    val storage =
      mockk<Storage> {
        every { list(any<String>(), any()) } returns page
      }
    val gcsLogClientFactory =
      mockk<GcsLogClientFactory> {
        every { get() } returns storage
      }
    val storageBucketConfig =
      mockk<StorageBucketConfig> {
        every { log } returns "bucket"
      }
    val client =
      GscLogClient(
        gcsClientFactory = gcsLogClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        jobLoggingCloudPrefix = "cloud-prefix",
      )
    client.deleteLogs(logPath = "log-path")
    verify(exactly = 1) { blob.delete(any()) }
  }

  @Test
  internal fun testTailingCloudLogs() {
    val numLines = 100
    val logPath = "log-path"

    val blob =
      mockk<Blob> {
        every { downloadTo(any<OutputStream>()) } answers {
          val stream = it.invocation.args.first() as OutputStream
          stream.write("some text".toByteArray())
        }
      }
    val page =
      mockk<Page<Blob>> {
        every { iterateAll() } returns (1..numLines).map { blob }.toList()
      }
    val storage =
      mockk<Storage> {
        every { list(any<String>(), any()) } returns page
      }
    val gcsLogClientFactory =
      mockk<GcsLogClientFactory> {
        every { get() } returns storage
      }
    val storageBucketConfig =
      mockk<StorageBucketConfig> {
        every { log } returns "bucket"
      }
    val client =
      GscLogClient(
        gcsClientFactory = gcsLogClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        jobLoggingCloudPrefix = "cloud-prefix",
      )
    val lines = client.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(numLines, lines.size)
  }
}
