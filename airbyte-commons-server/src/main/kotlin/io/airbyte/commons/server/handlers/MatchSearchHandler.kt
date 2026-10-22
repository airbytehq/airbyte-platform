/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.ConnectionSearch
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.ConnectionMatcher
import io.airbyte.commons.server.handlers.helpers.DestinationMatcher
import io.airbyte.commons.server.handlers.helpers.SourceMatcher
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import jakarta.inject.Inject
import jakarta.inject.Singleton

/**
 * Handles matching connections to search criteria.
 */
@Singleton
open class MatchSearchHandler
  @Inject
  constructor(
    private val destinationHandler: DestinationHandler,
    private val sourceHandler: SourceHandler,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val connectionService: ConnectionService,
    private val apiPojoConverters: ApiPojoConverters,
  ) {
    fun matchSearch(
      connectionSearch: ConnectionSearch?,
      connectionRead: ConnectionRead,
    ): Boolean {
      val sourceConnection = sourceService.getSourceConnection(connectionRead.sourceId)
      val sourceDefinition =
        sourceService.getStandardSourceDefinition(sourceConnection.sourceDefinitionId)
      val sourceRead = sourceHandler.toSourceRead(sourceConnection, sourceDefinition)

      val destinationConnection = destinationService.getDestinationConnection(connectionRead.destinationId)
      val destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationConnection.destinationDefinitionId)
      val destinationRead = destinationHandler.toDestinationRead(destinationConnection, destinationDefinition)

      val connectionMatcher = ConnectionMatcher(connectionSearch)
      val connectionReadFromSearch = connectionMatcher.match(connectionRead)

      return connectionReadFromSearch == connectionRead &&
        // ConnectionSearch is still a Java model; its source and destination filters are converted here
        matchSearch(Jsons.convertValue(connectionSearch?.source, SourceSearch::class.java), sourceRead) &&
        matchSearch(Jsons.convertValue(connectionSearch?.destination, DestinationSearch::class.java), destinationRead)
    }

    fun searchConnections(connectionSearch: ConnectionSearch?): ConnectionReadList {
      val reads: MutableList<ConnectionRead> = mutableListOf()
      for (standardSync in connectionService.listStandardSyncs()) {
        if (standardSync.status != StandardSync.Status.DEPRECATED) {
          val connectionRead = apiPojoConverters.internalToConnectionRead(standardSync)
          if (matchSearch(connectionSearch, connectionRead)) {
            reads.add(connectionRead)
          }
        }
      }

      return ConnectionReadList(connections = reads)
    }

    companion object {
      fun matchSearch(
        sourceSearch: SourceSearch?,
        sourceRead: SourceRead,
      ): Boolean {
        val sourceMatcher = SourceMatcher(sourceSearch)
        val sourceReadFromSearch = sourceMatcher.match(sourceRead)

        return sourceReadFromSearch == sourceRead
      }

      fun matchSearch(
        destinationSearch: DestinationSearch?,
        destinationRead: DestinationRead,
      ): Boolean {
        val destinationMatcher = DestinationMatcher(destinationSearch)
        val destinationReadFromSearch = destinationMatcher.match(destinationRead)

        return destinationReadFromSearch == destinationRead
      }
    }
  }
