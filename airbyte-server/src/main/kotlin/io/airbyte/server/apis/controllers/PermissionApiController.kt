/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.PermissionApi
import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionReadList
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.api.model.generated.UserIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.Permission
import io.airbyte.server.apis.execute
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/permissions")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class PermissionApiController(
  private val permissionHandler: PermissionHandler,
) : PermissionApi {
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN, AuthRoleConstants.WORKSPACE_ADMIN)
  @Post("/create")
  @AuditLogging(provider = AuditLoggingProvider.CREATE_PERMISSION)
  override fun createPermission(
    @Body permissionCreate: PermissionCreate,
  ): PermissionRead? =
    execute {
      validatePermissionCreation(permissionCreate)
      permissionHandler.createPermission(permissionCreate.toDomain()).toApi()
    }

  private fun validatePermissionCreation(permissionCreate: PermissionCreate) {
    if (permissionCreate.permissionType == PermissionType.INSTANCE_ADMIN) {
      throw JsonValidationException("Instance Admin permissions cannot be created via API.")
    }
    if (permissionCreate.organizationId == null && permissionCreate.workspaceId == null) {
      throw JsonValidationException("Either workspaceId or organizationId should be provided.")
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.WORKSPACE_READER)
  @Post("/get")
  override fun getPermission(
    @Body permissionIdRequestBody: PermissionIdRequestBody,
  ): PermissionRead? = execute { permissionHandler.getPermissionRead(permissionIdRequestBody) }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN, AuthRoleConstants.WORKSPACE_ADMIN)
  @Post("/update")
  @AuditLogging(provider = AuditLoggingProvider.UPDATE_PERMISSION)
  override fun updatePermission(
    @Body permissionUpdate: PermissionUpdate,
  ) {
    execute<Any?> {
      validatePermissionUpdate(permissionUpdate)
      permissionHandler.updatePermission(permissionUpdate)
      null
    }
  }

  private fun validatePermissionUpdate(
    @Body permissionUpdate: PermissionUpdate,
  ) {
    if (permissionUpdate.permissionType == PermissionType.INSTANCE_ADMIN) {
      throw JsonValidationException("Cannot modify Instance Admin permissions via API.")
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN, AuthRoleConstants.WORKSPACE_ADMIN)
  @Post("/delete")
  @AuditLogging(provider = AuditLoggingProvider.DELETE_PERMISSION)
  override fun deletePermission(
    @Body permissionIdRequestBody: PermissionIdRequestBody,
  ) {
    execute<Any?> {
      permissionHandler.deletePermission(permissionIdRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN, AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.SELF)
  @Post("/delete_user_from_workspace")
  override fun deleteUserFromWorkspace(
    @Body permissionDeleteUserFromWorkspaceRequestBody: PermissionDeleteUserFromWorkspaceRequestBody,
  ) {
    execute<Any?> {
      permissionHandler.deleteUserFromWorkspace(permissionDeleteUserFromWorkspaceRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @Post("/list_by_user")
  override fun listPermissionsByUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ): PermissionReadList? = execute { permissionHandler.permissionReadListForUser(userIdRequestBody.userId) }

  @Secured(AuthRoleConstants.ADMIN) // instance admins only
  @Post("/check")
  override fun checkPermissions(
    @Body permissionCheckRequest: PermissionCheckRequest,
  ): PermissionCheckRead? = execute { permissionHandler.checkPermissions(permissionCheckRequest) }

  @Secured(AuthRoleConstants.ADMIN) // instance admins only
  @Post("/check_multiple_workspaces")
  override fun checkPermissionsAcrossMultipleWorkspaces(
    @Body request: PermissionsCheckMultipleWorkspacesRequest,
  ): PermissionCheckRead? = execute { permissionHandler.permissionsCheckMultipleWorkspaces(request) }
}

private fun PermissionCreate.toDomain() =
  Permission()
    .withPermissionId(this.permissionId)
    .withUserId(this.userId)
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)
    .withPermissionType(enumValueOf(this.permissionType.name))

private fun Permission.toApi() =
  PermissionRead()
    .permissionId(this.permissionId)
    .userId(this.userId)
    .workspaceId(this.workspaceId)
    .organizationId(this.organizationId)
    .permissionType(enumValueOf(this.permissionType.name))
