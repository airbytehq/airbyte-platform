package io.airbyte.commons.audit

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PermissionRead
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("createPermission")
class CreatePermissionAuditProvider(
  private val helper: AuditProviderHelper,
) : AuditProvider {
  override fun generateSummary(result: Any?): String {
    if (result is PermissionRead) {
      val permissionLogEntry =
        AuditPermissionLogEntry(
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
    return ""
  }
}
