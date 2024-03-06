/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.filters

import io.airbyte.api.model.generated.JobStatus
import io.airbyte.public_api.model.generated.JobStatusEnum
import io.airbyte.public_api.model.generated.JobTypeEnum
import java.time.OffsetDateTime

/**
 * Filters for jobs. Does some conversion.
 */
class JobsFilter(
  createdAtStart: OffsetDateTime?,
  createdAtEnd: OffsetDateTime?,
  updatedAtStart: OffsetDateTime?,
  updatedAtEnd: OffsetDateTime?,
  limit: Int? = 20,
  offset: Int? = 0,
  val jobType: JobTypeEnum?,
  private val status: JobStatusEnum?,
) :
  BaseFilter(createdAtStart, createdAtEnd, updatedAtStart, updatedAtEnd, limit, offset) {
  /**
   * Convert Airbyte API job status to config API job status.
   */
  fun getConfigApiStatuses(): List<JobStatus>? = if (status == null) null else JobStatus.entries.filter { it.name == status.name }
}
