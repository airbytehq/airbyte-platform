/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.storage

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ObjectStoragePathResolverTest {
  private val objectMapper = ObjectMapper()

  @Nested
  inner class S3RejectedRecordsResolution {
    @Test
    fun `resolve returns correct metadata for valid S3 config without custom endpoint`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
          put("s3_bucket_region", "us-west-2")
          put("bucket_path", "my/path")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(123L)
      assertNotNull(result)
      assertEquals("s3://my-bucket/my/path/123/", result!!.storageUri)
      assertEquals(
        "https://us-west-2.console.aws.amazon.com/s3/buckets/my-bucket?prefix=my%2Fpath%2F123%2F",
        result.cloudConsoleUrl,
      )
    }

    @Test
    fun `resolve returns null if bucket name is missing`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_region", "us-west-2")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(123L)
      assertNull(result)
    }

    @Test
    fun `resolve returns null if bucket region is missing`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(123L)
      assertNull(result)
    }

    @Test
    fun `resolve returns null cloudConsoleUrl if custom endpoint is present`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
          put("s3_bucket_region", "us-west-2")
          put("bucket_path", "my/path")
          put("s3_endpoint", "https://custom.endpoint")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(123L)
      assertNotNull(result)
      assertEquals("s3://my-bucket/my/path/123/", result!!.storageUri)
      assertNull(result.cloudConsoleUrl)
    }

    @Test
    fun `resolve uses jobId as path if bucket_path is missing or empty`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
          put("s3_bucket_region", "us-west-2")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(456L)
      assertNotNull(result)
      assertEquals("s3://my-bucket/456/", result!!.storageUri)
      assertEquals(
        "https://us-west-2.console.aws.amazon.com/s3/buckets/my-bucket?prefix=456%2F",
        result.cloudConsoleUrl,
      )
    }

    @Test
    fun `resolve handles bucket_path with trailing slash correctly`() {
      val config =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
          put("s3_bucket_region", "us-west-2")
          put("bucket_path", "my/path/")
        }
      val resolver = S3ObjectStoragePathResolver(config)
      val result = resolver.resolveRejectedRecordsPaths(123L)
      assertNotNull(result)
      assertEquals("s3://my-bucket/my/path/123/", result!!.storageUri)
      assertEquals(
        "https://us-west-2.console.aws.amazon.com/s3/buckets/my-bucket?prefix=my%2Fpath%2F123%2F",
        result.cloudConsoleUrl,
      )
    }
  }
}
