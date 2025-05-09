/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.model

data class AuditPermissionLogEntry(
  val targetUser: TargetUser,
  val targetScope: TargetScope,
  val previousRole: String? = null,
  val newRole: String? = null,
)

data class TargetUser(
  val id: String,
  val email: String? = null,
)

data class TargetScope(
  val type: String,
  val id: String,
)
