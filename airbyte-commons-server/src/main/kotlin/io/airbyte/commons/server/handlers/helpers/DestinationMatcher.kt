/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSearch

/**
 * Constructs a query for finding a query.
 */
class DestinationMatcher(
  private val search: DestinationSearch?,
) : Matchable<DestinationRead> {
  override fun match(query: DestinationRead): DestinationRead {
    if (search == null) {
      return query
    }

    return DestinationRead().apply {
      name(search.name?.takeIf { it.isNotBlank() } ?: query.name)
      destinationDefinitionId(search.destinationDefinitionId ?: query.destinationDefinitionId)
      destinationId(search.destinationId ?: query.destinationId)
      destinationName(search.destinationName?.takeIf { it.isNotBlank() } ?: query.destinationName)
      workspaceId(search.workspaceId ?: query.workspaceId)
      icon(query.icon)
      isVersionOverrideApplied(query.isVersionOverrideApplied)
      breakingChanges(query.breakingChanges)
      supportState(query.supportState)
      resourceAllocation(query.resourceAllocation)

      val connectionConfiguration =
        when {
          search.connectionConfiguration == null -> query.connectionConfiguration
          query.connectionConfiguration == null -> search.connectionConfiguration
          else -> {
            val connCfg = search.connectionConfiguration

            query.connectionConfiguration.fieldNames().forEachRemaining { field ->
              if (!connCfg.has(field) && connCfg is ObjectNode) {
                connCfg.set<JsonNode>(field, query.connectionConfiguration[field])
              }
            }

            connCfg
          }
        }

      connectionConfiguration(connectionConfiguration)
    }
  }
}
