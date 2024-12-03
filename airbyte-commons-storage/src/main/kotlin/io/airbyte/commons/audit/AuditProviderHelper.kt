package io.airbyte.commons.audit

import io.airbyte.api.model.generated.PermissionRead
import jakarta.inject.Singleton

@Singleton
class AuditProviderHelper {
  fun getPermissionScope(permissionRead: PermissionRead): String {
    return when {
      permissionRead.organizationId != null -> "organization"
      permissionRead.workspaceId != null -> "workspace"
      else -> "instance"
    }
  }
}
