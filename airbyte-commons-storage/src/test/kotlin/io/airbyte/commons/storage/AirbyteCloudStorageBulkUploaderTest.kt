/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.mockk.every
import io.mockk.mockk
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class AirbyteCloudStorageBulkUploaderTest {
  private lateinit var storageClient: StorageClient

  @BeforeEach
  fun setup() {
    storageClient = mockk()
  }

  @AfterEach
  fun teardown() {
    unmockkAll()
  }

  @Test
  fun `upload converts the buffer to json and calls write from the storage client`() {
    val baseStorageId = "/objects"
    val period = 1L
    val unit = TimeUnit.SECONDS

    every { storageClient.write(any(), any()) } returns Unit

    val uploader =
      AirbyteCloudStorageBulkUploader<String>(
        baseStorageId,
        storageClient,
        period,
        unit,
      )

    uploader.start()
    uploader.append("message-1")
    Thread.sleep(TimeUnit.SECONDS.toMillis(period * 2))
    uploader.stop()

    verify(exactly = 1) { storageClient.write(any(), eq("[\"message-1\"]")) }
  }
}
