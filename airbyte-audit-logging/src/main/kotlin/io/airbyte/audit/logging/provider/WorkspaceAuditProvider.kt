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
 * Audit provider for the workspace endpoints.
 *
 * Their request bodies carry `webhookConfigs`, whose entries hold an `authToken`, so the request
 * cannot be logged wholesale. Both request and response also carry a large amount of workspace
 * preference state (notification settings, setup-wizard flags, dataplane group) that is noise in an
 * audit trail.
 *
 * Both sides are built from an explicit allowlist, so any field that is not allowlisted -- including
 * secret-bearing fields added to these bodies in the future -- is omitted by default. The response is
 * filtered too because that is where a create's newly assigned workspace id comes from.
 */
@Singleton
@Named(AuditLoggingProvider.WORKSPACE)
class WorkspaceAuditProvider : AuditProvider {
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
     * intentionally omitted -- notably `webhookConfigs`, whose entries carry an `authToken` on the
     * write side.
     */
    private val LOGGABLE_FIELDS =
      setOf(
        "workspaceId",
        "name",
        "email",
        "organizationId",
      )
  }
}
