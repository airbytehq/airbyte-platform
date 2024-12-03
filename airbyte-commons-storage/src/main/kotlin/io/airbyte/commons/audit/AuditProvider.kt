package io.airbyte.commons.audit

interface AuditProvider {
  fun generateSummary(result: Any?): String
}
