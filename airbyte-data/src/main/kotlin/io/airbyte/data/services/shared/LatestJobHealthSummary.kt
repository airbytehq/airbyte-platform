/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.JobStatus
import java.util.UUID

data class LatestJobHealthSummary(
  val scope: String,
  val status: JobStatus,
  val sourceDefinitionVersionId: UUID?,
  val destinationDefinitionVersionId: UUID?,
  val sourceDockerImageIsDefault: Boolean?,
  val destinationDockerImageIsDefault: Boolean?,
)
