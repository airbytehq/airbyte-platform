/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.server.repositories.domain.RetryState
import jakarta.inject.Singleton

/**
 * Maps between the API and Persistence layers. It is not static to be injectable and enable easier
 * testing in dependents.
 */
@Singleton
class RetryStatesMapper {
  // API to Domain
  fun map(api: JobRetryStateRequestBody): RetryState =
    RetryState
      .RetryStateBuilder()
      .id(api.id)
      .connectionId(api.connectionId)
      .jobId(api.jobId)
      .successiveCompleteFailures(api.successiveCompleteFailures)
      .totalCompleteFailures(api.totalCompleteFailures)
      .successivePartialFailures(api.successivePartialFailures)
      .totalPartialFailures(api.totalPartialFailures)
      .build()

  // Domain to API
  fun map(domain: RetryState): RetryStateRead =
    RetryStateRead()
      .id(domain.id)
      .connectionId(domain.connectionId)
      .jobId(domain.jobId)
      .successiveCompleteFailures(domain.successiveCompleteFailures)
      .totalCompleteFailures(domain.totalCompleteFailures)
      .successivePartialFailures(domain.successivePartialFailures)
      .totalPartialFailures(domain.totalPartialFailures)
}
