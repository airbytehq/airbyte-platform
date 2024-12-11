package io.airbyte.audit.logging

interface AuditProvider {
  companion object {
    const val EMPTY_SUMMARY = "{}"
  }

  fun generateSummaryFromRequest(request: Any?): String

  fun generateSummaryFromResult(result: Any?): String
}
