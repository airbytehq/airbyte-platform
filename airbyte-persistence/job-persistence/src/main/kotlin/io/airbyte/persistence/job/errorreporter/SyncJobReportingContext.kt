/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import java.util.UUID

/**
 * Sync Job Reporting Context.
 *
 * @param jobId job id.
 * @param sourceVersionId source definition version id. Can be null in the case of resets.
 * @param destinationVersionId destination definition version id.
 */

data class SyncJobReportingContext(
  val jobId: Long,
  val sourceVersionId: UUID?,
  val destinationVersionId: UUID?,
)
