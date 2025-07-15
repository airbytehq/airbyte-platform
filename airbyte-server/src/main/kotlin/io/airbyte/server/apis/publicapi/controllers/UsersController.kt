/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicUsersApi
import io.airbyte.publicApi.server.generated.models.UsersResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.USERS_PATH
import io.airbyte.server.apis.publicapi.services.UserService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class UsersController(
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
) : PublicUsersApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListUsersWithinAnOrganization(
    organizationId: String,
    ids: List<String>?,
    emails: List<String>?,
  ): Response {
    // You need to have an organization_member or a higher role to get all users within the same organization.
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_MEMBER)

    // Process and monitor the request.
    val usersResponse: UsersResponse =
      trackingHelper.callWithTracker(
        {
          userService.getUsersInAnOrganization(UUID.fromString(organizationId), ids, emails)
        },
        USERS_PATH,
        GET,
        currentUserService.getCurrentUser().userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(usersResponse)
      .build()
  }
}
