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

@SuppressWarnings("MissingJavadocType")
@JdbcRepository(dialect = Dialect.POSTGRES)
public interface RetryStatesRepository extends PageableRepository<RetryState, UUID> {

  Optional<RetryState> findByJobId(final Long jobId);

  void updateByJobId(final Long jobId, final RetryState update);

}
