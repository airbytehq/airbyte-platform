package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Named("deletePermission")
class DeletePermissionAuditProvider(
  private val helper: AuditLoggingHelper,
) : AuditProvider {
  override fun generateSummaryFromRequest(request: Any?): String {
    try {
      if (request is PermissionIdRequestBody) {
        val permissionToDelete = helper.getPermission(request.permissionId)
        val permissionLogEntry =
          AuditPermissionLogEntry(
            // Todo: get user email from id
            targetUser = TargetUser(id = permissionToDelete.userId.toString()),
            previousRole = permissionToDelete.permissionType.toString(),
            newRole = null,
            targetScope =
              TargetScope(
                type = helper.getPermissionScope(permissionToDelete),
                id = permissionToDelete.organizationId?.toString() ?: permissionToDelete.workspaceId?.toString() ?: "",
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
    // There is no result returned for deleting permission request
    return AuditProvider.EMPTY_SUMMARY
  }
}
