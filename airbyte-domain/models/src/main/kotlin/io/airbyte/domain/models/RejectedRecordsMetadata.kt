/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RejectedRecordsMetadata(
  /**
   * Link to the cloud console for the storage bucket containing rejected records.
   * This helps users navigate to the bucket directly via the cloud provider's console UI.
   */
  val cloudConsoleUrl: String? = null,
  /**
   * URI to the storage bucket path containing rejected records.
   * This is typically in the format `s3://bucket-name/path/to/job-id`.
   */
  val storageUri: String? = null,
)
