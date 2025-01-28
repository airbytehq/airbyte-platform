/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.handlers.helpers.ConnectionMatcher;
import io.airbyte.commons.server.handlers.helpers.DestinationMatcher;
import io.airbyte.commons.server.handlers.helpers.SourceMatcher;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;

/**
 * Handles matching connections to search criteria.
 */
@Singleton
public class MatchSearchHandler {

  private final DestinationHandler destinationHandler;
  private final SourceHandler sourceHandler;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final ConnectionService connectionService;
  private final ApiPojoConverters apiPojoConverters;

  @Inject
  public MatchSearchHandler(final DestinationHandler destinationHandler,
                            final SourceHandler sourceHandler,
                            final SourceService sourceService,
                            final DestinationService destinationService,
                            final ConnectionService connectionService,
                            final ApiPojoConverters apiPojoConverters) {
    this.destinationHandler = destinationHandler;
    this.sourceHandler = sourceHandler;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.connectionService = connectionService;
    this.apiPojoConverters = apiPojoConverters;
  }

  public static boolean matchSearch(final SourceSearch sourceSearch, final SourceRead sourceRead) {
    final SourceMatcher sourceMatcher = new SourceMatcher(sourceSearch);
    final SourceRead sourceReadFromSearch = sourceMatcher.match(sourceRead);

    return (sourceReadFromSearch == null || sourceReadFromSearch.equals(sourceRead));
  }

  public static boolean matchSearch(final DestinationSearch destinationSearch, final DestinationRead destinationRead) {
    final DestinationMatcher destinationMatcher = new DestinationMatcher(destinationSearch);
    final DestinationRead destinationReadFromSearch = destinationMatcher.match(destinationRead);

    return (destinationReadFromSearch == null || destinationReadFromSearch.equals(destinationRead));
  }

  public boolean matchSearch(final ConnectionSearch connectionSearch, final ConnectionRead connectionRead)
      throws JsonValidationException, ConfigNotFoundException, IOException {

    final SourceConnection sourceConnection = sourceService.getSourceConnection(connectionRead.getSourceId());
    final StandardSourceDefinition sourceDefinition =
        sourceService.getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final SourceRead sourceRead = sourceHandler.toSourceRead(sourceConnection, sourceDefinition);

    final DestinationConnection destinationConnection = destinationService.getDestinationConnection(connectionRead.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationConnection.getDestinationDefinitionId());
    final DestinationRead destinationRead = destinationHandler.toDestinationRead(destinationConnection, destinationDefinition);

    final ConnectionMatcher connectionMatcher = new ConnectionMatcher(connectionSearch);
    final ConnectionRead connectionReadFromSearch = connectionMatcher.match(connectionRead);

    return (connectionReadFromSearch == null || connectionReadFromSearch.equals(connectionRead))
        && matchSearch(connectionSearch.getSource(), sourceRead)
        && matchSearch(connectionSearch.getDestination(), destinationRead);
  }

  public ConnectionReadList searchConnections(final ConnectionSearch connectionSearch)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConnectionRead> reads = Lists.newArrayList();
    for (final StandardSync standardSync : connectionService.listStandardSyncs()) {
      if (standardSync.getStatus() != StandardSync.Status.DEPRECATED) {
        final ConnectionRead connectionRead = apiPojoConverters.internalToConnectionRead(standardSync);
        if (matchSearch(connectionSearch, connectionRead)) {
          reads.add(connectionRead);
        }
      }
    }

    return new ConnectionReadList().connections(reads);
  }

}
