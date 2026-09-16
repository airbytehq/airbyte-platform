/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.audit.logging.model.Actor
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.micronaut.http.HttpHeaders
import io.micronaut.security.utils.SecurityService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class AuditLoggingHelper(
  private val permissionHandler: PermissionHandler,
  private val currentUserService: CurrentUserService,
  private val objectMapper: ObjectMapper,
  // In community auth, where micronaut security is disabled, the security service isn't available.
  private val securityService: SecurityService?,
) {
  fun buildActor(headers: HttpHeaders): Actor? {
    // Instance admins act on behalf of Airbyte itself; the user-based actor is not valid for them.
    if (securityService?.hasRole(AuthRoleConstants.ADMIN) == true) {
      return Actor(actorId = AIRBYTE_SUPPORT_ACTOR_ID)
    }

    val currentUser =
      try {
        currentUserService.getCurrentUser()
      } catch (_: Exception) {
        null
      } ?: return null

    val userAgent = headers.get("User-Agent")?.takeIf { it.isNotEmpty() } ?: "unknown"
    val ipAddress = headers.get("X-Forwarded-For")?.takeIf { it.isNotEmpty() } ?: "unknown"

    return Actor(
      actorId = currentUser.userId.toString(),
      email = currentUser.email,
      ipAddress = ipAddress,
      userAgent = userAgent,
    )
  }

  companion object {
    const val AIRBYTE_SUPPORT_ACTOR_ID = "Airbyte Support"
  }

  fun getPermission(permissionId: UUID): Permission = permissionHandler.getPermissionById(permissionId)

  fun getPermissionScope(permissionRead: PermissionRead): String =
    when {
      permissionRead.organizationId != null -> "organization"
      permissionRead.workspaceId != null -> "workspace"
      else -> "instance"
    }

  fun getPermissionScope(permission: Permission): String =
    when {
      permission.organizationId != null -> "organization"
      permission.workspaceId != null -> "workspace"
      else -> "instance"
    }

  fun generateSummary(
    requestSummary: String,
    resultSummary: String,
  ): String {
    val requestJsonNode: JsonNode = objectMapper.readTree(requestSummary)
    val resultJsonNode: JsonNode = objectMapper.readTree(resultSummary)

    // Create a copy of the requestJsonNode
    val mergedJsonNode = requestJsonNode.deepCopy() as ObjectNode

    // Merge the resultJsonNode
    if (resultJsonNode is ObjectNode) {
      resultJsonNode.fieldNames().forEachRemaining { fieldName ->
        mergedJsonNode.set<JsonNode>(fieldName, resultJsonNode.get(fieldName))
      }
    }

    // Return the merged JSON as a string
    return objectMapper.writeValueAsString(mergedJsonNode)
  }
}
