/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSearch

/**
 * Constructs a query for finding a query.
 */
class ConnectionMatcher(
  private val search: ConnectionSearch?,
) : Matchable<ConnectionRead> {
  override fun match(query: ConnectionRead): ConnectionRead {
    if (search == null) {
      return query
    }

    return ConnectionRead().apply {
      connectionId(search.connectionId ?: query.connectionId)
      destinationId(search.destinationId ?: query.destinationId)
      name(search.name?.takeIf { it.isNotBlank() } ?: query.name)
      namespaceFormat(search.namespaceFormat?.takeIf { it.isNotBlank() && it != "null" } ?: query.namespaceFormat)
      namespaceDefinition(search.namespaceDefinition ?: query.namespaceDefinition)
      prefix(search.prefix?.takeIf { it.isNotBlank() } ?: query.prefix)
      schedule(search.schedule ?: query.schedule)
      scheduleType(search.scheduleType ?: query.scheduleType)
      scheduleData(search.scheduleData ?: query.scheduleData)
      sourceId(search.sourceId ?: query.sourceId)
      status(search.status ?: query.status)

      // these properties are not enabled in the search
      resourceRequirements(query.resourceRequirements)
      syncCatalog(query.syncCatalog)
      operationIds(query.operationIds)
      sourceCatalogId(query.sourceCatalogId)
      dataplaneGroupId(query.dataplaneGroupId)
      breakingChange(query.breakingChange)
      notifySchemaChanges(query.notifySchemaChanges)
      notifySchemaChangesByEmail(query.notifySchemaChangesByEmail)
      backfillPreference(query.backfillPreference)
    }
  }
}
