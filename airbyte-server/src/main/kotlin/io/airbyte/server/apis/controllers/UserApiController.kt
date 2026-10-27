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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.GlobalAdminRule
import io.airbyte.server.auth.OrganizationMemberRule
import io.airbyte.server.auth.UserEmailRule
import io.airbyte.server.auth.UserOnboardingRule
import io.airbyte.server.auth.UserSelfRule
import io.airbyte.server.auth.UserWorkspaceReadRule
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserSelfRule::class,
  )
  override fun getUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ): UserRead = execute { userHandler.getUser(userIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserSelfRule::class,
  )
  override fun getUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody,
  ): UserRead = execute { userHandler.getUserByAuthId(userAuthIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserEmailRule::class,
  )
  override fun getUserByEmail(
    @Body userEmailRequestBody: UserEmailRequestBody,
  ): UserRead = execute { userHandler.getUserByEmail(userEmailRequestBody) }

  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserSelfRule::class,
  )
  override fun deleteUser(
    @Body userIdRequestBody: UserIdRequestBody,
  ) {
    execute<Any?> {
      userHandler.deleteUser(userIdRequestBody)
      null
    }
  }

  @AuditLogging(AuditLoggingProvider.ONLY_ACTOR)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserSelfRule::class,
  )
  override fun updateUser(
    @Body userUpdate: UserUpdate,
  ): UserRead = execute { userHandler.updateUser(userUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationMemberRule::class,
  )
  override fun listUsersInOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationUserReadList =
    execute {
      userHandler.listUsersInOrganization(organizationIdRequestBody ?: throw BadRequestException("Request body is required."))
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserWorkspaceReadRule::class,
  )
  override fun listAccessInfoByWorkspaceId(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceUserAccessInfoReadList =
    execute {
      userHandler.listAccessInfoByWorkspaceId(
        workspaceIdRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = GlobalAdminRule::class,
  )
  override fun listInstanceAdminUsers(): UserWithPermissionInfoReadList = execute { userHandler.listInstanceAdminUsers() }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = UserOnboardingRule::class,
  )
  override fun getOrCreateUserByAuthId(
    @Body userAuthIdRequestBody: UserAuthIdRequestBody,
  ): UserGetOrCreateByAuthIdResponse = execute { userHandler.getOrCreateUserByAuthId(userAuthIdRequestBody) }
}
