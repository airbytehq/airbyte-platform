package io.airbyte.audit.logging

import java.util.UUID

data class AuditLogEntry(
  val id: UUID,
  val timestamp: Long,
  val user: User? = null,
  val actionName: String,
  val summary: String,
  val success: Boolean,
  val errorMessage: String? = null,
)
