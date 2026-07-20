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

@Singleton
@Named(AuditLoggingProvider.SCIM)
class ScimAuditProvider : AuditProvider {
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
    private val LOGGABLE_FIELDS = setOf("organizationId", "idpProvider")
  }
}
