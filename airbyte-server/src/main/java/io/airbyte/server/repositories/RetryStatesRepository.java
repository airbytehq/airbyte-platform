/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories;

import io.airbyte.server.repositories.domain.RetryState;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.PageableRepository;
import java.util.Optional;
import java.util.UUID;

@JdbcRepository(dialect = Dialect.POSTGRES,
                dataSource = "config")
public interface RetryStatesRepository extends PageableRepository<RetryState, UUID> {

  Optional<RetryState> findByJobId(final Long jobId);

  void updateByJobId(final Long jobId, final RetryState update);

  boolean existsByJobId(final long jobId);

  default void createOrUpdateByJobId(final long jobId, final RetryState payload) {
    final var exists = existsByJobId(jobId);

    if (exists) {
      updateByJobId(jobId, payload);
    } else {
      save(payload);
    }
  }

}
