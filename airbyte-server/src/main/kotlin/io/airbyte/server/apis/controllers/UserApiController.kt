/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.UserApi
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationUserReadList
import io.airbyte.api.server.generated.models.UserAuthIdRequestBody
import io.airbyte.api.server.generated.models.UserEmailRequestBody
import io.airbyte.api.server.generated.models.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.server.generated.models.UserIdRequestBody
import io.airbyte.api.server.generated.models.UserRead
import io.airbyte.api.server.generated.models.UserUpdate
import io.airbyte.api.server.generated.models.UserWithPermissionInfoReadList
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * User related APIs. TODO: migrate all User endpoints (including some endpoints in WebBackend API)
 * from Cloud to OSS.
 */
@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class UserApiController(
  private val userHandler: UserHandler,
) : UserApi {
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ): UserRead = execute { userHandler.getUser(userIdRequestBody) }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody,
  ): UserRead = execute { userHandler.getUserByAuthId(userAuthIdRequestBody) }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUserByEmail(
    @Body userEmailRequestBody: UserEmailRequestBody,
  ): UserRead = execute { userHandler.getUserByEmail(userEmailRequestBody) }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  override fun deleteUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ) {
    execute<Any?> {
      userHandler.deleteUser(userIdRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  override fun updateUser(
    @Body userUpdate: UserUpdate,
  ): UserRead = execute { userHandler.updateUser(userUpdate) }

  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listUsersInOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationUserReadList =
    execute {
      userHandler.listUsersInOrganization(organizationIdRequestBody ?: throw BadRequestException("Request body is required."))
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAccessInfoByWorkspaceId(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceUserAccessInfoReadList =
    execute {
      userHandler.listAccessInfoByWorkspaceId(
        workspaceIdRequestBody,
      )
    }

  @Secured(AuthRoleConstants.ADMIN) // instance admin only
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listInstanceAdminUsers(): UserWithPermissionInfoReadList = execute { userHandler.listInstanceAdminUsers() }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrCreateUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody,
  ): UserGetOrCreateByAuthIdResponse = execute { userHandler.getOrCreateUserByAuthId(userAuthIdRequestBody) }
}
