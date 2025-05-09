/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.commons.annotation.AuditLoggingProvider
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Audit log provider that omits the request and response from the log. This can be used to skip logging
 * large request/response bodies, which may make the audit log more difficult to parse.
 */
@Singleton
@Named(AuditLoggingProvider.ONLY_ACTOR)
class OnlyActorAuditProvider : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String = AuditProvider.EMPTY_SUMMARY

  override fun generateSummaryFromResult(result: Any?): String = AuditProvider.EMPTY_SUMMARY
}
