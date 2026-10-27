/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.PermissionApi
import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.PermissionCheckRequest
import io.airbyte.api.server.generated.models.PermissionCreate
import io.airbyte.api.server.generated.models.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.server.generated.models.PermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionRead
import io.airbyte.api.server.generated.models.PermissionReadList
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.PermissionUpdate
import io.airbyte.api.server.generated.models.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.api.server.generated.models.UserIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.Permission
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.PermissionCreateRule
import io.airbyte.server.auth.PermissionDeleteRule
import io.airbyte.server.auth.PermissionDiagnosticRule
import io.airbyte.server.auth.PermissionLeaveWorkspaceRule
import io.airbyte.server.auth.PermissionReadRule
import io.airbyte.server.auth.PermissionUpdateRule
import io.airbyte.server.auth.PermissionUserListRule
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class PermissionApiController(
  private val permissionHandler: PermissionHandler,
) : PermissionApi {
  @AuditLogging(provider = AuditLoggingProvider.CREATE_PERMISSION)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionCreateRule::class,
  )
  override fun createPermission(
    @Body permissionCreate: PermissionCreate,
  ): PermissionRead =
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

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionReadRule::class,
  )
  override fun getPermission(
    @Body permissionIdRequestBody: PermissionIdRequestBody,
  ): PermissionRead = execute { permissionHandler.getPermissionRead(permissionIdRequestBody) }

  @AuditLogging(provider = AuditLoggingProvider.UPDATE_PERMISSION)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionUpdateRule::class,
  )
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

  @AuditLogging(provider = AuditLoggingProvider.DELETE_PERMISSION)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionDeleteRule::class,
  )
  override fun deletePermission(
    @Body permissionIdRequestBody: PermissionIdRequestBody,
  ) {
    execute<Any?> {
      permissionHandler.deletePermission(permissionIdRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionLeaveWorkspaceRule::class,
  )
  override fun deleteUserFromWorkspace(
    @Body permissionDeleteUserFromWorkspaceRequestBody: PermissionDeleteUserFromWorkspaceRequestBody,
  ) {
    execute<Any?> {
      permissionHandler.deleteUserFromWorkspace(permissionDeleteUserFromWorkspaceRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionUserListRule::class,
  )
  override fun listPermissionsByUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ): PermissionReadList = execute { permissionHandler.effectivePermissionReadListForUser(userIdRequestBody.userId) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionDiagnosticRule::class,
  )
  override fun checkPermissions(
    @Body permissionCheckRequest: PermissionCheckRequest,
  ): PermissionCheckRead = execute { permissionHandler.checkPermissions(permissionCheckRequest) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PermissionDiagnosticRule::class,
  )
  override fun checkPermissionsAcrossMultipleWorkspaces(
    @Body request: PermissionsCheckMultipleWorkspacesRequest,
  ): PermissionCheckRead = execute { permissionHandler.permissionsCheckMultipleWorkspaces(request) }
}

private fun PermissionCreate.toDomain() =
  Permission()
    .withPermissionId(this.permissionId)
    .withUserId(this.userId)
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)
    .withPermissionType(enumValueOf(this.permissionType.name))

private fun Permission.toApi() =
  PermissionRead(
    permissionId = this.permissionId,
    userId = this.userId,
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
    permissionType = enumValueOf(this.permissionType.name),
  )
