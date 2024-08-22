/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

internal class S3LogClientTest {
  @Test
  internal fun testLogDeletion() {
    val obj =
      mockk<S3Object> {
        every { key() } returns "key"
      }
    val page =
      mockk<ListObjectsV2Response> {
        every { contents() } returns listOf(obj, obj)
      }
    val iterable =
      mockk<ListObjectsV2Iterable> {
        every { iterator() } returns mutableListOf(page).iterator()
      }
    val s3Client =
      mockk<S3Client> {
        every { deleteObjects(any<DeleteObjectsRequest>()) } returns mockk<DeleteObjectsResponse>()
        every { listObjectsV2Paginator(any<ListObjectsV2Request>()) } returns iterable
      }
    val storageBucketConfig =
      mockk<StorageBucketConfig> {
        every { log } returns "bucket"
      }
    val s3LogClientFactory =
      mockk<S3LogClientFactory> {
        every { get() } returns s3Client
      }
    val client =
      S3LogClient(
        s3LogClientFactory = s3LogClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        storageType = LogClientType.S3.name.lowercase(),
        jobLoggingCloudPrefix = "cloud-prefix",
      )

    client.deleteLogs(logPath = "log-path")
    verify(exactly = 1) { s3Client.deleteObjects(any<DeleteObjectsRequest>()) }
  }

  @Test
  internal fun testTailingCloudLogs() {
    val numLines = 100
    val logPath = "log-path"

    val obj =
      mockk<S3Object> {
        every { key() } returns "key"
      }
    val keys = listOf(obj, obj)
    val page =
      mockk<ListObjectsV2Response> {
        every { contents() } returns keys
      }
    val iterable =
      mockk<ListObjectsV2Iterable> {
        every { iterator() } returns mutableListOf(page).iterator()
      }
    val responseBytes =
      mockk<ResponseBytes<GetObjectResponse>> {
        every { asByteArray() } returns "a log line".toByteArray()
      }
    val s3Client =
      mockk<S3Client> {
        every { getObjectAsBytes(any<GetObjectRequest>()) } returns responseBytes
        every { listObjectsV2Paginator(any<ListObjectsV2Request>()) } returns iterable
      }
    val storageBucketConfig =
      mockk<StorageBucketConfig> {
        every { log } returns "bucket"
      }
    val s3LogClientFactory =
      mockk<S3LogClientFactory> {
        every { get() } returns s3Client
      }
    val client =
      S3LogClient(
        s3LogClientFactory = s3LogClientFactory,
        storageBucketConfig = storageBucketConfig,
        meterRegistry = null,
        storageType = LogClientType.S3.name.lowercase(),
        jobLoggingCloudPrefix = "cloud-prefix",
      )

    val lines = client.tailCloudLogs(logPath = logPath, numLines = numLines)
    assertEquals(keys.size, lines.size)
  }
}
