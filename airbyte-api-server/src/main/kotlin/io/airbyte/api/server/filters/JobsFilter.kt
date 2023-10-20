/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.filters

import io.airbyte.airbyte_api.model.generated.JobStatusEnum
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.api.client.model.generated.JobStatus
import io.micronaut.core.annotation.Nullable
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
  jobType: JobTypeEnum?,
  status: JobStatusEnum?,
) :
  BaseFilter(createdAtStart, createdAtEnd, updatedAtStart, updatedAtEnd, limit, offset) {
  val jobType: JobTypeEnum?
  private val status: JobStatusEnum?

  init {
    this.jobType = jobType
    this.status = status
  }

  /**
   * Convert Airbyte API job status to config API job status.
   */
  @Nullable
  fun getConfigApiStatus(): JobStatus? {
    return if (status == null) {
      null
    } else {
      JobStatus.fromValue(status.toString())
    }
  }
//    @Nullable fun getJobType(): JobTypeEnum {
//        return jobType
//    }
}
