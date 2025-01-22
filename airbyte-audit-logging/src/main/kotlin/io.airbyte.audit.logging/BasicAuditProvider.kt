package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.annotation.AuditLoggingProvider
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Basic audit provider that simply logs the original request and the result.
 */
@Singleton
@Named(AuditLoggingProvider.BASIC)
class BasicAuditProvider : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String {
    return ObjectMapper().writeValueAsString(request)
  }

  override fun generateSummaryFromResult(result: Any?): String {
    return ObjectMapper().writeValueAsString(result)
  }
}
