/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter.model

data class LongRunningJobMetadata(
  @JvmField val sourceDockerImage: String,
  @JvmField val destinationDockerImage: String,
  @JvmField val workspaceId: String,
  @JvmField val connectionId: String,
)
