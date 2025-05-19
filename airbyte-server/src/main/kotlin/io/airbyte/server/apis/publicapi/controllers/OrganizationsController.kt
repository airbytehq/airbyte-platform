/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicOrganizationsApi
import io.airbyte.publicApi.server.generated.models.ActorTypeEnum
import io.airbyte.publicApi.server.generated.models.OrganizationOAuthCredentialsRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.ORGANIZATIONS_PATH
import io.airbyte.server.apis.publicapi.constants.PUT
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
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
  private val roleResolver: RoleResolver,
) : PublicOrganizationsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun createOrUpdateOrganizationOAuthCredentials(
    organizationId: String,
    organizationOAuthCredentialsRequest: OrganizationOAuthCredentialsRequest,
  ): Response {
    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    trackingHelper.callWithTracker(
      {
        licenseEntitlementChecker.ensureEntitled(
          UUID.fromString(organizationId),
          Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
        )

        val actorType =
          when (organizationOAuthCredentialsRequest.actorType) {
            ActorTypeEnum.SOURCE -> io.airbyte.api.model.generated.ActorTypeEnum.SOURCE
            ActorTypeEnum.DESTINATION -> io.airbyte.api.model.generated.ActorTypeEnum.DESTINATION
          }

        organizationService.setOrganizationOverrideOauthParams(
          OrganizationId(UUID.fromString(organizationId)),
          organizationOAuthCredentialsRequest.name,
          actorType,
          organizationOAuthCredentialsRequest.configuration,
        )
      },
      ORGANIZATIONS_PATH,
      PUT,
      currentUserService.currentUser.userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .build()
  }

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
