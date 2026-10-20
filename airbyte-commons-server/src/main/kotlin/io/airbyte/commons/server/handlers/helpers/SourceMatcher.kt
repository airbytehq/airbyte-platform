/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceSearch

/**
 * Constructs a query for finding a query.
 */
class SourceMatcher(
  private val search: SourceSearch?,
) : Matchable<SourceRead> {
  override fun match(query: SourceRead): SourceRead {
    if (search == null) {
      return query
    }

    val connCfg = search.connectionConfiguration
    val connectionConfiguration =
      when {
        connCfg == null -> query.connectionConfiguration
        else -> {
          query.connectionConfiguration.fieldNames().forEachRemaining { field ->
            if (!connCfg.has(field) && connCfg is ObjectNode) {
              connCfg.set<JsonNode>(field, query.connectionConfiguration[field])
            }
          }

          connCfg
        }
      }

    // createdAt is required on the Kotlin model, so it is carried over like the other read-only fields
    return SourceRead(
      name = search.name?.takeIf { it.isNotBlank() } ?: query.name,
      sourceDefinitionId = search.sourceDefinitionId ?: query.sourceDefinitionId,
      sourceId = search.sourceId ?: query.sourceId,
      sourceName = search.sourceName?.takeIf { it.isNotBlank() } ?: query.sourceName,
      workspaceId = search.workspaceId ?: query.workspaceId,
      icon = query.icon,
      isVersionOverrideApplied = query.isVersionOverrideApplied,
      isEntitled = query.isEntitled,
      breakingChanges = query.breakingChanges,
      supportState = query.supportState,
      isDraft = query.isDraft,
      resourceAllocation = query.resourceAllocation,
      createdAt = query.createdAt,
      connectionConfiguration = connectionConfiguration,
    )
  }
}
