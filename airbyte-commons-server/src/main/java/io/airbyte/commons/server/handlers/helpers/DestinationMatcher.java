/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationSearch;
import org.apache.commons.lang3.StringUtils;

/**
 * Constructs a query for finding a query.
 */
public class DestinationMatcher implements Matchable<DestinationRead> {

  private final DestinationSearch search;

  public DestinationMatcher(final DestinationSearch search) {
    this.search = search;
  }

  @Override
  public DestinationRead match(final DestinationRead query) {
    if (search == null) {
      return query;
    }

    final DestinationRead fromSearch = new DestinationRead();
    fromSearch.name(StringUtils.isBlank(search.getName()) ? query.getName() : search.getName());
    fromSearch.destinationDefinitionId(search.getDestinationDefinitionId() == null ? query.getDestinationDefinitionId()
        : search.getDestinationDefinitionId());
    fromSearch
        .destinationId(search.getDestinationId() == null ? query.getDestinationId() : search.getDestinationId());
    fromSearch.destinationName(
        StringUtils.isBlank(search.getDestinationName()) ? query.getDestinationName() : search.getDestinationName());
    fromSearch.workspaceId(search.getWorkspaceId() == null ? query.getWorkspaceId() : search.getWorkspaceId());
    fromSearch.icon(query.getIcon());
    fromSearch.isVersionOverrideApplied(query.getIsVersionOverrideApplied());
    fromSearch.breakingChanges(query.getBreakingChanges());
    fromSearch.supportState(query.getSupportState());

    if (search.getConnectionConfiguration() == null) {
      fromSearch.connectionConfiguration(query.getConnectionConfiguration());
    } else if (query.getConnectionConfiguration() == null) {
      fromSearch.connectionConfiguration(search.getConnectionConfiguration());
    } else {
      final JsonNode connectionConfiguration = search.getConnectionConfiguration();
      query.getConnectionConfiguration().fieldNames()
          .forEachRemaining(field -> {
            if (!connectionConfiguration.has(field) && connectionConfiguration instanceof ObjectNode) {
              ((ObjectNode) connectionConfiguration).set(field, query.getConnectionConfiguration().get(field));
            }
          });
      fromSearch.connectionConfiguration(connectionConfiguration);
    }

    return fromSearch;
  }

}
