/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_GROUP_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.IS_PUBLIC_API_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.config.ScopeType
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * Resolves organization or workspace IDs from HTTP headers.
 */
@Singleton
class AuthenticationHeaderResolver(
  private val workspaceHelper: WorkspaceHelper,
  private val permissionHandler: PermissionHandler,
  private val userPersistence: UserPersistence?,
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneService: DataplaneService,
) {
  /**
   * Resolve corresponding organization ID based on headers that were set by the AuthorizationServerHandler
   */
  @Nullable
  fun resolveOrganization(properties: Map<String, String>): List<UUID>? {
    log.debug { "properties: $properties" }
    try {
      if (properties.containsKey(ORGANIZATION_ID_HEADER)) {
        return listOf<UUID>(UUID.fromString(properties[ORGANIZATION_ID_HEADER]))
      } else if (properties.containsKey(SCOPE_TYPE_HEADER) &&
        properties.containsKey(SCOPE_ID_HEADER) &&
        properties[SCOPE_TYPE_HEADER].equals(ScopeType.ORGANIZATION.value(), ignoreCase = true)
      ) {
        // if the scope type is organization, we can use the scope id directly to resolve an organization
        // id.
        val organizationId = properties[SCOPE_ID_HEADER]
        return listOf(UUID.fromString(organizationId))
      } else if (properties.containsKey(DATAPLANE_GROUP_ID_HEADER)) {
        val organizationId = resolveOrganizationIdFromDataplaneGroupHeader(properties[DATAPLANE_GROUP_ID_HEADER])
        if (organizationId != null) {
          return listOf(organizationId)
        }
      } else if (properties.containsKey(DATAPLANE_ID_HEADER)) {
        val organizationId = resolveOrganizationIdFromDataplaneHeader(properties[DATAPLANE_ID_HEADER])
        if (organizationId != null) {
          return listOf(organizationId)
        }
      } else {
        // resolving by permission id requires a database fetch, so we
        // handle it last and with a dedicated check to minimize latency.
        val organizationId = resolveOrganizationIdFromPermissionHeader(properties)
        if (organizationId != null) {
          return listOf(organizationId)
        }
      }
      // Else, determine the organization from workspace related fields.
      val workspaceIds = resolveWorkspace(properties) ?: return null

      val organizationIds: MutableList<UUID> = ArrayList()
      for (workspaceId in workspaceIds) {
        try {
          organizationIds.add(workspaceHelper.getOrganizationForWorkspace(workspaceId))
        } catch (e: Exception) {
          log.debug { "Unable to resolve organization ID for workspace ID: $workspaceId" }
        }
      }
      return organizationIds
    } catch (e: IllegalArgumentException) {
      log.debug(e) { "Unable to resolve organization ID." }
      return null
    } catch (e: ConfigNotFoundException) {
      log.debug(e) { "Unable to resolve organization ID." }
      return null
    } catch (e: io.airbyte.config.persistence.ConfigNotFoundException) {
      log.debug(e) { "Unable to resolve organization ID." }
      return null
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  /**
   * Resolves workspaces from header.
   */
  @Nullable // This is an indication that the workspace ID as a group for auth needs refactoring
  fun resolveWorkspace(properties: Map<String, String>): List<UUID>? {
    log.debug { "properties: $properties" }
    try {
      if (properties.containsKey(WORKSPACE_ID_HEADER)) {
        val workspaceId = properties[WORKSPACE_ID_HEADER]
        return listOf(UUID.fromString(workspaceId))
      } else if (properties.containsKey(CONNECTION_ID_HEADER)) {
        val connectionId = properties[CONNECTION_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(connectionId)))
      } else if (properties.containsKey(CONNECTION_IDS_HEADER)) {
        return resolveConnectionIds(properties)
      } else if (properties.containsKey(SOURCE_ID_HEADER) && properties.containsKey(DESTINATION_ID_HEADER)) {
        val destinationId = properties[DESTINATION_ID_HEADER]
        val sourceId = properties[SOURCE_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForConnection(UUID.fromString(sourceId), UUID.fromString(destinationId)))
      } else if (properties.containsKey(DESTINATION_ID_HEADER)) {
        val destinationId = properties[DESTINATION_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForDestinationId(UUID.fromString(destinationId)))
      } else if (properties.containsKey(JOB_ID_HEADER)) {
        val jobId = properties[JOB_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForJobId(jobId!!.toLong()))
      } else if (properties.containsKey(SOURCE_ID_HEADER)) {
        val sourceId = properties[SOURCE_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForSourceId(UUID.fromString(sourceId)))
      } else if (properties.containsKey(OPERATION_ID_HEADER)) {
        val operationId = properties[OPERATION_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForOperationId(UUID.fromString(operationId)))
      } else if (properties.containsKey(CONFIG_ID_HEADER)) {
        val configId = properties[CONFIG_ID_HEADER]
        return listOf(workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(configId)))
      } else if (properties.containsKey(WORKSPACE_IDS_HEADER)) {
        // If workspaceIds were passed in as empty list [], they apparently don't show up in the headers so
        // this will be skipped
        // The full list of workspace ID permissions is handled below in the catch-all.
        return resolveWorkspaces(properties)
      } else if (properties.containsKey(SCOPE_TYPE_HEADER) &&
        properties.containsKey(SCOPE_ID_HEADER) &&
        properties[SCOPE_TYPE_HEADER].equals(ScopeType.WORKSPACE.value(), ignoreCase = true)
      ) {
        // if the scope type is workspace, we can use the scope id directly to resolve a workspace id.
        val workspaceId = properties[SCOPE_ID_HEADER]
        return listOf(UUID.fromString(workspaceId))
      } else if (!properties.containsKey(WORKSPACE_IDS_HEADER) && properties.containsKey(IS_PUBLIC_API_HEADER)) {
        // If the WORKSPACE_IDS_HEADER is missing and this is a public API request, we should return empty
        // list so that we pass through
        // the permission check and the controller/handler can either pull all workspaces the user has
        // access to or fail.
        return emptyList()
      } else {
        // resolving by permission id requires a database fetch, so we
        // handle it last and with a dedicated check to minimize latency.
        val workspaceId = resolveWorkspaceIdFromPermissionHeader(properties)
        if (workspaceId != null) {
          return listOf(workspaceId)
        }

        log.debug { "Request does not contain any headers that resolve to a workspace ID." }
        return null
      }
    } catch (e: IllegalArgumentException) {
      log.debug(e) { "Unable to resolve workspace ID." }
      return null
    } catch (e: JsonValidationException) {
      log.debug(e) { "Unable to resolve workspace ID." }
      return null
    } catch (e: ConfigNotFoundException) {
      log.debug(e) { "Unable to resolve workspace ID." }
      return null
    } catch (e: io.airbyte.config.persistence.ConfigNotFoundException) {
      log.debug(e) { "Unable to resolve workspace ID." }
      return null
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  @Nullable
  fun resolveAuthUserIds(properties: Map<String?, String?>): Set<String>? {
    log.debug { "properties: $properties" }
    try {
      if (properties.containsKey(EXTERNAL_AUTH_ID_HEADER)) {
        val authUserId = properties[EXTERNAL_AUTH_ID_HEADER]
        return authUserId?.let { setOf(it) }
      } else if (properties.containsKey(AIRBYTE_USER_ID_HEADER)) {
        return resolveAirbyteUserIdToAuthUserIds(properties[AIRBYTE_USER_ID_HEADER]!!)
      } else if (properties.containsKey(CREATOR_USER_ID_HEADER)) {
        return resolveAirbyteUserIdToAuthUserIds(properties[CREATOR_USER_ID_HEADER]!!)
      } else {
        log.debug { "Request does not contain any headers that resolve to a user ID." }
        return null
      }
    } catch (e: Exception) {
      log.debug(e) { "Unable to resolve user ID." }
      return null
    }
  }

  private fun resolveAirbyteUserIdToAuthUserIds(airbyteUserId: String): Set<String> {
    val authUserIds = userPersistence?.listAuthUserIdsForUser(UUID.fromString(airbyteUserId)) ?: emptySet()

    require(!authUserIds.isEmpty()) { String.format("Could not find any authUserIds for userId %s", airbyteUserId) }

    return HashSet(authUserIds)
  }

  private fun resolveWorkspaceIdFromPermissionHeader(properties: Map<String, String>): UUID? {
    if (!properties.containsKey(PERMISSION_ID_HEADER)) {
      return null
    }
    val permission =
      permissionHandler.getPermissionRead(
        PermissionIdRequestBody().permissionId(UUID.fromString(properties[PERMISSION_ID_HEADER])),
      )
    return permission.workspaceId
  }

  private fun resolveOrganizationIdFromPermissionHeader(properties: Map<String, String>): UUID? {
    if (!properties.containsKey(PERMISSION_ID_HEADER)) {
      return null
    }
    val permission =
      permissionHandler.getPermissionRead(
        PermissionIdRequestBody().permissionId(UUID.fromString(properties[PERMISSION_ID_HEADER])),
      )
    return permission.organizationId
  }

  private fun resolveWorkspaces(properties: Map<String, String>): List<UUID>? {
    val workspaceIds: String? = properties[WORKSPACE_IDS_HEADER]
    log.debug { "workspaceIds from header: $workspaceIds" }
    if (workspaceIds != null) {
      // todo (cgardens) -- spooky
      val deserialized: List<String> =
        Jsons.deserialize(
          workspaceIds,
          object : TypeReference<List<String>>() {},
        )
      return deserialized.stream().map { name: String? -> UUID.fromString(name) }.toList()
    }
    log.debug { "Request does not contain any headers that resolve to a list of workspace IDs." }
    return null
  }

  private fun resolveConnectionIds(properties: Map<String, String>): List<UUID>? {
    val connectionIds = properties[CONNECTION_IDS_HEADER]
    if (connectionIds != null) {
      val deserialized: List<String> =
        Jsons.deserialize(
          connectionIds,
          object : TypeReference<List<String>>() {},
        )
      return deserialized
        .stream()
        .map { connectionId: String? ->
          try {
            return@map workspaceHelper.getWorkspaceForConnectionId(UUID.fromString(connectionId))
          } catch (e: JsonValidationException) {
            throw RuntimeException(e)
          } catch (e: ConfigNotFoundException) {
            throw RuntimeException(e)
          }
        }.toList()
    }
    return null
  }

  private fun resolveOrganizationIdFromDataplaneGroupHeader(dataplaneGroupHeaderValue: String?): UUID? {
    if (dataplaneGroupHeaderValue == null) {
      return null
    }
    return try {
      val dataplaneGroupId = UUID.fromString(dataplaneGroupHeaderValue)
      dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      log.debug("Unable to resolve organization ID from dataplane group header.", e)
      null
    }
  }

  private fun resolveOrganizationIdFromDataplaneHeader(dataplaneHeaderValue: String?): UUID? {
    if (dataplaneHeaderValue == null) {
      return null
    }
    return try {
      val dataplaneId = UUID.fromString(dataplaneHeaderValue)
      val dataplaneGroupId = dataplaneService.getDataplane(dataplaneId).dataplaneGroupId
      dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      log.debug("Unable to resolve organization ID from dataplane header.", e)
      null
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
