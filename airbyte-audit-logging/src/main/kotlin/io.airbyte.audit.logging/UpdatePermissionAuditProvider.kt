package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PermissionUpdate
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Named("updatePermission")
class UpdatePermissionAuditProvider(
  private val helper: AuditLoggingHelper,
) : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String {
    try {
      if (request is PermissionUpdate) {
        val previousPermission = helper.getPermission(request.permissionId)
        val permissionLogEntry =
          AuditPermissionLogEntry(
            targetUser = TargetUser(id = previousPermission.userId.toString()),
            previousRole = previousPermission.permissionType.toString(),
            newRole = request.permissionType.toString(),
            targetScope =
              TargetScope(
                type = helper.getPermissionScope(previousPermission),
                id =
                  previousPermission.organizationId?.toString()
                    ?: previousPermission.workspaceId?.toString()
                    ?: "",
              ),
          )
        return ObjectMapper().writeValueAsString(permissionLogEntry)
      }
      return AuditProvider.EMPTY_SUMMARY
    } catch (e: Exception) {
      logger.error { "Failed to generate summary from request. Error: ${e.message}" }
      return AuditProvider.EMPTY_SUMMARY
    }
  }

  override fun generateSummaryFromResult(result: Any?): String {
    // There is no result returned for updating permission request
    return AuditProvider.EMPTY_SUMMARY
  }
}
