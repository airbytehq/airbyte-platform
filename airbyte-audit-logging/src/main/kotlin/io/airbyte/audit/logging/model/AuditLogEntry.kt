/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.model

import java.util.UUID

data class AuditLogEntry(
  val id: UUID,
  val timestamp: Long,
  val actor: Actor? = null,
  val operation: String,
  val request: Any? = null,
  val response: Any? = null,
  val success: Boolean,
  val errorMessage: String? = null,
)
