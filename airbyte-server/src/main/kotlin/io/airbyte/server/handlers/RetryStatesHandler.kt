/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.server.handlers.apidomainmapping.RetryStatesMapper
import io.airbyte.server.repositories.RetryStatesRepository
import jakarta.inject.Singleton

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
class RetryStatesHandler(
  val repo: RetryStatesRepository,
  val mapper: RetryStatesMapper,
) {
  fun getByJobId(req: JobIdRequestBody): RetryStateRead? = repo.findByJobId(req.id)?.let { mapper.map(it) }

  fun putByJobId(req: JobRetryStateRequestBody) {
    val model = mapper.map(req)

    repo.createOrUpdateByJobId(model.jobId!!, model)
  }
}
