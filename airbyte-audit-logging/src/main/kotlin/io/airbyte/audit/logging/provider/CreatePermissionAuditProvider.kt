/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.audit.logging.AuditLoggingHelper
import io.airbyte.audit.logging.model.AuditPermissionLogEntry
import io.airbyte.audit.logging.model.TargetScope
import io.airbyte.audit.logging.model.TargetUser
import io.airbyte.commons.annotation.AuditLoggingProvider
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named(AuditLoggingProvider.CREATE_PERMISSION)
class CreatePermissionAuditProvider(
  private val helper: AuditLoggingHelper,
) : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String = AuditProvider.EMPTY_SUMMARY

  override fun generateSummaryFromResult(result: Any?): String {
    if (result is PermissionRead) {
      val permissionLogEntry =
        AuditPermissionLogEntry(
          // Todo: get user email from id
          targetUser = TargetUser(id = result.userId.toString()),
          previousRole = null,
          newRole = result.permissionType.toString(),
          targetScope =
            TargetScope(
              type = helper.getPermissionScope(result),
              id = result.organizationId?.toString() ?: result.workspaceId?.toString() ?: "",
            ),
        )
      return ObjectMapper().writeValueAsString(permissionLogEntry)
    }
    return AuditProvider.EMPTY_SUMMARY
  }
}
