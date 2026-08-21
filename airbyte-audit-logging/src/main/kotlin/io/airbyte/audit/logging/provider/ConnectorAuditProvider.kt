/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.json.Jsons
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Audit provider for the configured source and destination endpoints -- not connector definitions.
 *
 * Their request and response bodies both carry `connectionConfiguration`, which holds raw connector
 * credentials on the create/update path, so neither side can be logged wholesale. Recording only the
 * actor instead loses which source or destination was acted on, since [io.airbyte.audit.logging.model.AuditLogEntry]
 * has no target-resource field.
 *
 * Both sides are therefore built from an explicit allowlist of identifying, non-secret fields, so any
 * field that is not allowlisted -- including secret-bearing fields added to these bodies in the future --
 * is omitted by default. The response is filtered too because that is where a create's newly assigned
 * id comes from.
 */
@Singleton
@Named(AuditLoggingProvider.CONNECTOR)
class ConnectorAuditProvider : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String = summarize(request)

  override fun generateSummaryFromResult(result: Any?): String = summarize(result)

  private fun summarize(body: Any?): String {
    if (body == null) {
      return AuditProvider.EMPTY_SUMMARY
    }

    val bodyNode = Jsons.jsonNode(body)
    val summary = Jsons.emptyObject() as ObjectNode
    LOGGABLE_FIELDS.forEach { field ->
      if (bodyNode.hasNonNull(field)) {
        summary.set<JsonNode>(field, bodyNode.get(field))
      }
    }
    return Jsons.serialize(summary)
  }

  companion object {
    /**
     * Identifying fields that are safe to persist to the audit log. Any field absent from this set is
     * intentionally omitted -- notably `connectionConfiguration` and `secretId`, which carry
     * credentials, and `resourceAllocation`, which is configuration rather than identity.
     */
    private val LOGGABLE_FIELDS =
      setOf(
        "sourceId",
        "destinationId",
        "sourceDefinitionId",
        "destinationDefinitionId",
        "sourceName",
        "destinationName",
        "name",
        "workspaceId",
      )
  }
}
