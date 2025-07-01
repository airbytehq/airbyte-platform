/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.storage

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.domain.models.RejectedRecordsMetadata

interface ObjectStoragePathResolver {
  fun resolveRejectedRecordsPaths(jobId: Long): RejectedRecordsMetadata?
}

class S3ObjectStoragePathResolver(
  private val objectStorageConfig: JsonNode,
) : ObjectStoragePathResolver {
  override fun resolveRejectedRecordsPaths(jobId: Long): RejectedRecordsMetadata? {
    val bucketName = objectStorageConfig.get("s3_bucket_name")?.takeIf { it.isTextual }?.asText()
    val bucketRegion = objectStorageConfig.get("s3_bucket_region")?.takeIf { it.isTextual }?.asText()

    if (bucketName == null || bucketRegion == null) {
      return null
    }

    val bucketPath = objectStorageConfig.get("bucket_path")?.takeIf { it.isTextual }?.asText()
    val s3Endpoint = objectStorageConfig.get("s3_endpoint")

    val jobPath = if (bucketPath.isNullOrEmpty()) jobId.toString() else "$bucketPath/$jobId"
    val storageUri = "s3://$bucketName/$jobPath"

    val s3ConsoleUrl =
      if (s3Endpoint == null || s3Endpoint.isNull) {
        "https://$bucketRegion.console.aws.amazon.com/s3/buckets/$bucketName?prefix=$jobPath"
      } else {
        // Custom S3 endpoints not supported for bucket links
        null
      }

    return RejectedRecordsMetadata(
      cloudConsoleUrl = s3ConsoleUrl,
      storageUri = storageUri,
    )
  }
}
