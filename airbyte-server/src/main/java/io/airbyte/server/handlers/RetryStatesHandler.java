/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.server.handlers.api_domain_mapping.RetryStatesMapper;
import io.airbyte.server.repositories.RetryStatesRepository;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
public class RetryStatesHandler {

  final RetryStatesRepository repo;
  final RetryStatesMapper mapper;

  public RetryStatesHandler(final RetryStatesRepository repo, final RetryStatesMapper mapper) {
    this.repo = repo;
    this.mapper = mapper;
  }

  public Optional<RetryStateRead> getByJobId(final JobIdRequestBody req) {
    final var found = repo.findByJobId(req.getId());

    return found.map(mapper::map);
  }

  public void putByJobId(final JobRetryStateRequestBody req) {
    final var model = mapper.map(req);

    repo.createOrUpdateByJobId(model.getJobId(), model);
  }

}
