/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicOrganizationsApi
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.ORGANIZATIONS_PATH
import io.airbyte.server.apis.publicapi.services.OrganizationService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationsController(
  private val organizationService: OrganizationService,
  private val trackingHelper: TrackingHelper,
  private val currentUserService: CurrentUserService,
) : PublicOrganizationsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListOrganizationsForUser(): Response {
    val userId: UUID = currentUserService.currentUser.userId
    val organizationsResponse =
      trackingHelper.callWithTracker(
        {
          organizationService.getOrganizationsByUser(userId)
        },
        ORGANIZATIONS_PATH,
        GET,
        userId,
      )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(organizationsResponse)
      .build()
  }
}
