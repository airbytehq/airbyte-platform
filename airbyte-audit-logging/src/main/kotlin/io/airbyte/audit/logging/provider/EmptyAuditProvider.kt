/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.commons.annotation.AuditLoggingProvider
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named(AuditLoggingProvider.EMPTY)
class EmptyAuditProvider : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String = AuditProvider.EMPTY_SUMMARY

  override fun generateSummaryFromResult(result: Any?): String = AuditProvider.EMPTY_SUMMARY
}
