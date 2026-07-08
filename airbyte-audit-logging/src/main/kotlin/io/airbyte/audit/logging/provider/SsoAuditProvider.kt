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
 * Audit provider for the SSO config endpoints. Their request bodies carry secret-bearing fields
 * (e.g. `clientSecret`, `accessToken`) that must never be persisted to the audit-log bucket.
 *
 * The request summary is built from an explicit allowlist of loggable fields, so any field that is
 * not allowlisted -- including secret-bearing fields added to these request bodies in the future --
 * is omitted by default.
 */
@Singleton
@Named(AuditLoggingProvider.SSO)
class SsoAuditProvider : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String {
    if (request == null) {
      return AuditProvider.EMPTY_SUMMARY
    }

    val requestNode = Jsons.jsonNode(request)
    val summary = Jsons.emptyObject() as ObjectNode
    LOGGABLE_FIELDS.forEach { field ->
      if (requestNode.hasNonNull(field)) {
        summary.set<JsonNode>(field, requestNode.get(field))
      }
    }
    return Jsons.serialize(summary)
  }

  override fun generateSummaryFromResult(result: Any?): String = AuditProvider.EMPTY_SUMMARY

  companion object {
    /**
     * Non-secret fields that are safe to persist to the audit log. Any field absent from this set
     * (e.g. `clientSecret`, `accessToken`) is intentionally omitted from the audit entry.
     */
    private val LOGGABLE_FIELDS =
      setOf(
        "organizationId",
        "companyIdentifier",
        "clientId",
        "discoveryUrl",
        "emailDomain",
        "emailDomains",
        "status",
        "defaultRole",
      )
  }
}
