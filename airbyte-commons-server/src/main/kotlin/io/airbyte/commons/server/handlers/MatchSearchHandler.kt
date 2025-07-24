/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.collect.Lists
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ConnectionSearch
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.ConnectionMatcher
import io.airbyte.commons.server.handlers.helpers.DestinationMatcher
import io.airbyte.commons.server.handlers.helpers.SourceMatcher
import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.io.IOException

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
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
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

      return (connectionReadFromSearch == null || connectionReadFromSearch == connectionRead) &&
        matchSearch(connectionSearch?.source, sourceRead) &&
        matchSearch(connectionSearch?.destination, destinationRead)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun searchConnections(connectionSearch: ConnectionSearch?): ConnectionReadList {
      val reads: MutableList<ConnectionRead> = Lists.newArrayList()
      for (standardSync in connectionService.listStandardSyncs()) {
        if (standardSync.status != StandardSync.Status.DEPRECATED) {
          val connectionRead = apiPojoConverters.internalToConnectionRead(standardSync)
          if (matchSearch(connectionSearch, connectionRead)) {
            reads.add(connectionRead)
          }
        }
      }

      return ConnectionReadList().connections(reads)
    }

    companion object {
      fun matchSearch(
        sourceSearch: SourceSearch?,
        sourceRead: SourceRead,
      ): Boolean {
        val sourceMatcher = SourceMatcher(sourceSearch)
        val sourceReadFromSearch = sourceMatcher.match(sourceRead)

        return (sourceReadFromSearch == null || sourceReadFromSearch == sourceRead)
      }

      fun matchSearch(
        destinationSearch: DestinationSearch?,
        destinationRead: DestinationRead,
      ): Boolean {
        val destinationMatcher = DestinationMatcher(destinationSearch)
        val destinationReadFromSearch = destinationMatcher.match(destinationRead)

        return (destinationReadFromSearch == null || destinationReadFromSearch == destinationRead)
      }
    }
  }
