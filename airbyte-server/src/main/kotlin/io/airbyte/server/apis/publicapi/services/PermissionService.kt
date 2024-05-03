/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.public_api.model.generated.PermissionCreateRequest
import io.airbyte.public_api.model.generated.PermissionResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.PermissionReadMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory

interface PermissionService {
  fun createPermission(permissionCreateRequest: PermissionCreateRequest): PermissionResponse
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
      kotlin.runCatching { permissionHandler.createPermission(permissionCreateOss) }
        .onFailure {
          log.error("Error for createPermission", it)
          ConfigClientErrorHandler.handleError(it, permissionCreateRequest.userId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return PermissionReadMapper.from(result.getOrNull()!!)
  }
}
