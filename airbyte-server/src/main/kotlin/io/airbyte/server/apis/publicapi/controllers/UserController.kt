/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicUsersApi
import io.airbyte.public_api.model.generated.UsersResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.USERS_PATH
import io.airbyte.server.apis.publicapi.services.UserService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(USERS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class UserController(
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicUsersApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListUsers(
    ids: List<UUID>?,
    emails: List<String>?,
    organizationId: UUID,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    if (!ids.isNullOrEmpty() && !emails.isNullOrEmpty()) {
      val badRequestProblem = BadRequestProblem("We only allow filtering users either on ID(s) or email(s), not both.", null)
      trackingHelper.trackFailuresIfAny(
        USERS_PATH,
        GET,
        userId,
        badRequestProblem,
      )
      throw badRequestProblem
    }
    // Auth check. To further process the getUsers request we require an organization_admin or a higher role.
    apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(
      Scope.ORGANIZATION,
      listOf(organizationId.toString()),
      setOf(OrganizationAuthRole.ORGANIZATION_ADMIN),
    )
    // Process and monitor the request.
    val usersResponse: UsersResponse
    if (ids.isNullOrEmpty() && emails.isNullOrEmpty()) {
      // If there is no filters at all, we will list all users.
      usersResponse =
        trackingHelper.callWithTracker(
          {
            userService.getAllUsers(organizationId)
          },
          USERS_PATH,
          GET,
          userId,
        )
    } else if (!ids.isNullOrEmpty()) { // ID(s) are provided instead of emails.
      usersResponse =
        trackingHelper.callWithTracker(
          {
            userService.getUsersByUserIds(ids, organizationId)
          },
          USERS_PATH,
          GET,
          userId,
        )
    } else { // Email(s) are provided instead of ID(s).
      usersResponse =
        trackingHelper.callWithTracker(
          {
            userService.getUsersByUserEmails(emails!!, organizationId)
          },
          USERS_PATH,
          GET,
          userId,
        )
    }
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(usersResponse)
      .build()
  }
}
