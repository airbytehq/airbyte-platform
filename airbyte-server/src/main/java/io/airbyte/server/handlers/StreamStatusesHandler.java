/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.server.handlers.api_domain_mapping.StreamStatusesMapper;
import io.airbyte.server.repositories.StreamStatusesRepository;
import jakarta.inject.Singleton;

/**
 * Interface layer between the API and Persistence layers.
 */
@SuppressWarnings("MissingJavadocMethod")
@Singleton
public class StreamStatusesHandler {

  final StreamStatusesRepository repo;
  final StreamStatusesMapper mapper;

  public StreamStatusesHandler(final StreamStatusesRepository repo, final StreamStatusesMapper mapper) {
    this.repo = repo;
    this.mapper = mapper;
  }

  public StreamStatusRead createStreamStatus(final StreamStatusCreateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.save(model);

    return mapper.map(saved);
  }

  public StreamStatusRead updateStreamStatus(final StreamStatusUpdateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.update(model);

    return mapper.map(saved);
  }

  public StreamStatusReadList listStreamStatus(final StreamStatusListRequestBody req) {
    final var filters = mapper.map(req);

    final var page = repo.findAllFiltered(filters);

    final var apiList = page.getContent()
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

  public StreamStatusReadList listStreamStatusPerRunState(final ConnectionIdRequestBody req) {
    final var apiList = repo.findAllPerRunStateByConnectionId(req.getConnectionId())
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

}
