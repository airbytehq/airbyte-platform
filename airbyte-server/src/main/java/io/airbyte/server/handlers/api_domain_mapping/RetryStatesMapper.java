/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.server.repositories.domain.RetryState;
import jakarta.inject.Singleton;

/**
 * Maps between the API and Persistence layers. It is not static to be injectable and enable easier
 * testing in dependents.
 */
@Singleton
public class RetryStatesMapper {

  // API to Domain
  public RetryState map(final JobRetryStateRequestBody api) {
    return new RetryState.RetryStateBuilder()
        .id(api.getId())
        .connectionId(api.getConnectionId())
        .jobId(api.getJobId())
        .successiveCompleteFailures(api.getSuccessiveCompleteFailures())
        .totalCompleteFailures(api.getTotalCompleteFailures())
        .successivePartialFailures(api.getSuccessivePartialFailures())
        .totalPartialFailures(api.getTotalPartialFailures())
        .build();
  }

  // Domain to API
  public RetryStateRead map(final RetryState domain) {
    return new RetryStateRead()
        .id(domain.getId())
        .connectionId(domain.getConnectionId())
        .jobId(domain.getJobId())
        .successiveCompleteFailures(domain.getSuccessiveCompleteFailures())
        .totalCompleteFailures(domain.getTotalCompleteFailures())
        .successivePartialFailures(domain.getSuccessivePartialFailures())
        .totalPartialFailures(domain.getTotalPartialFailures());
  }

}
