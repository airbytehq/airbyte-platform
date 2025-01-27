/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.publicApi.server.generated.models.PermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.PermissionResponse
import io.airbyte.publicApi.server.generated.models.PermissionUpdateRequest
import io.airbyte.publicApi.server.generated.models.PermissionsResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.PermissionReadMapper
import io.airbyte.server.apis.publicapi.mappers.PermissionResponseReadMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface PermissionService {
  fun createPermission(permissionCreateRequest: PermissionCreateRequest): PermissionResponse

  fun getPermissionsByUserId(userId: UUID): PermissionsResponse

  fun getPermissionsByUserInAnOrganization(
    userId: UUID,
    organizationId: UUID,
  ): PermissionsResponse

  fun getPermission(permissionId: UUID): PermissionResponse

  fun updatePermission(
    permissionId: UUID,
    permissionUpdateRequest: PermissionUpdateRequest,
  ): PermissionResponse

  fun deletePermission(permissionId: UUID)
}

@Singleton
@Secondary
open class PermissionServiceImpl(
  private val permissionHandler: PermissionHandler,
  @Value("\${airbyte.api.host}") open val publicApiHost: String,
) : PermissionService {
  companion object {
    private val log = LoggerFactory.getLogger(PermissionServiceImpl::class.java)
  }

  /**
   * Creates a permission.
   */
  override fun createPermission(permissionCreateRequest: PermissionCreateRequest): PermissionResponse {
    val permissionCreateOss = PermissionCreate()
    permissionCreateOss.permissionType = enumValueOf(permissionCreateRequest.permissionType.name)
    permissionCreateOss.userId = permissionCreateRequest.userId
    permissionCreateOss.organizationId = permissionCreateRequest.organizationId
    permissionCreateOss.workspaceId = permissionCreateRequest.workspaceId

    val result =
      kotlin
        .runCatching { permissionHandler.createPermission(permissionCreateOss) }
        .onFailure {
          log.error("Error for createPermission", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return PermissionReadMapper.from(result.getOrThrow())
  }

  /**
   * Gets all permissions of a single user (by user ID).
   */
  override fun getPermissionsByUserId(userId: UUID): PermissionsResponse {
    val result =
      kotlin
        .runCatching { permissionHandler.listPermissionsByUser(userId) }
        .onFailure {
          log.error("Error for getPermissionsByUserId", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val permissionReadList = result.getOrThrow().permissions
    return PermissionsResponse(
      data = permissionReadList.mapNotNull { PermissionResponseReadMapper.from(it) },
    )
  }

  /**
   * Gets all permissions of a single user (by user ID) in a single organization.
   */
  override fun getPermissionsByUserInAnOrganization(
    userId: UUID,
    organizationId: UUID,
  ): PermissionsResponse {
    val result =
      kotlin
        .runCatching { permissionHandler.listPermissionsByUserInAnOrganization(userId, organizationId) }
        .onFailure {
          log.error("Error for getPermissionsByUserInAnOrganization", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    val permissionReadList = result.getOrThrow().permissions
    return PermissionsResponse(data = permissionReadList.mapNotNull { PermissionResponseReadMapper.from(it) })
  }

  /**
   * Gets a permission.
   */
  override fun getPermission(permissionId: UUID): PermissionResponse {
    val permissionIdRequestBody = PermissionIdRequestBody()
    permissionIdRequestBody.permissionId = permissionId
    val result =
      kotlin
        .runCatching { permissionHandler.getPermission(permissionIdRequestBody) }
        .onFailure {
          log.error("Error for getPermission", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return PermissionReadMapper.from(result.getOrThrow())
  }

  /**
   * Updates a permission.
   */
  override fun updatePermission(
    permissionId: UUID,
    permissionUpdateRequest: PermissionUpdateRequest,
  ): PermissionResponse {
    val permissionUpdate = PermissionUpdate()
    permissionUpdate.permissionId = permissionId
    permissionUpdate.permissionType = enumValueOf(permissionUpdateRequest.permissionType.name)
    val updatedPermission =
      kotlin
        .runCatching {
          permissionHandler.updatePermission(permissionUpdate)
          val updatedPermission = permissionHandler.getPermission(PermissionIdRequestBody().permissionId(permissionId))
          updatedPermission
        }.onFailure {
          log.error("Error for updatePermission", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + updatedPermission)
    return PermissionReadMapper.from(updatedPermission.getOrThrow())
  }

  /**
   * Deletes a permission.
   */
  override fun deletePermission(permissionId: UUID) {
    val permissionIdRequestBody = PermissionIdRequestBody()
    permissionIdRequestBody.permissionId = permissionId
    val result =
      kotlin
        .runCatching { permissionHandler.deletePermission(permissionIdRequestBody) }
        .onFailure {
          log.error("Error for deletePermission", it)
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
  }
}
