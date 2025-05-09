/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

interface AuditProvider {
  companion object {
    const val EMPTY_SUMMARY = "{}"
  }

  fun generateSummaryFromRequest(request: Any?): Any?

  fun generateSummaryFromResult(result: Any?): Any?
}
