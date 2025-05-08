/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.UserApi
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationUserReadList
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.api.model.generated.UserEmailRequestBody
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.model.generated.UserIdRequestBody
import io.airbyte.api.model.generated.UserRead
import io.airbyte.api.model.generated.UserUpdate
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

/**
 * User related APIs. TODO: migrate all User endpoints (including some endpoints in WebBackend API)
 * from Cloud to OSS.
 */
@Controller("/api/v1/users")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class UserApiController(
  private val userHandler: UserHandler,
) : UserApi {
  @Post("/get")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ): UserRead? = execute { userHandler.getUser(userIdRequestBody) }

  @Post("/get_by_auth_id")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody,
  ): UserRead? = execute { userHandler.getUserByAuthId(userAuthIdRequestBody) }

  @Post("/get_by_email")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  override fun getUserByEmail(
    @Body userEmailRequestBody: UserEmailRequestBody,
  ): UserRead? = execute { userHandler.getUserByEmail(userEmailRequestBody) }

  @Post("/delete")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  override fun deleteUser(
    @Body userIdRequestBody: UserIdRequestBody?,
  ) {
    execute<Any?> {
      userHandler.deleteUser(userIdRequestBody)
      null
    }
  }

  @Post("/update")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  override fun updateUser(
    @Body userUpdate: UserUpdate,
  ): UserRead? = execute { userHandler.updateUser(userUpdate) }

  @Post("/list_by_organization_id")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listUsersInOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationUserReadList? = execute { userHandler.listUsersInOrganization(organizationIdRequestBody) }

  @Post("/list_access_info_by_workspace_id")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAccessInfoByWorkspaceId(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceUserAccessInfoReadList? =
    execute {
      userHandler.listAccessInfoByWorkspaceId(
        workspaceIdRequestBody,
      )
    }

  @Post("/list_instance_admins")
  @Secured(AuthRoleConstants.ADMIN) // instance admin only
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listInstanceAdminUsers(): UserWithPermissionInfoReadList? = execute(Callable { userHandler.listInstanceAdminUsers() })

  @Post("/get_or_create_by_auth_id")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrCreateUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody?,
  ): UserGetOrCreateByAuthIdResponse? = execute { userHandler.getOrCreateUserByAuthId(userAuthIdRequestBody) }
}
