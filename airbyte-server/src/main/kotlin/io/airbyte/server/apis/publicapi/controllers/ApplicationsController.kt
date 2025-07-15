/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.ApplicationService
import io.airbyte.publicApi.server.generated.apis.PublicApplicationsApi
import io.airbyte.publicApi.server.generated.models.ApplicationCreate
import io.airbyte.publicApi.server.generated.models.ApplicationRead
import io.airbyte.publicApi.server.generated.models.ApplicationReadList
import io.airbyte.publicApi.server.generated.models.ApplicationTokenRequestWithGrant
import io.airbyte.publicApi.server.generated.models.PublicAccessTokenResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.APPLICATIONS_PATH
import io.airbyte.server.apis.publicapi.constants.APPLICATIONS_PATH_WITH_ID
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.mappers.toApplicationRead
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.Optional
import kotlin.time.Duration.Companion.minutes

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
@Requires(bean = ApplicationService::class)
open class ApplicationsController(
  private val applicationService: ApplicationService,
  private val tokenExpirationConfig: TokenExpirationConfig,
  private val currentUserService: CurrentUserService,
  private val trackingHelper: TrackingHelper,
) : PublicApplicationsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateApplication(applicationCreate: ApplicationCreate): Response {
    val user: AuthenticatedUser = currentUserService.getCurrentUser()

    // process and monitor the request
    val applicationRead =
      toApplicationRead(
        trackingHelper.callWithTracker(
          {
            applicationService.createApplication(user, applicationCreate.name)
          },
          APPLICATIONS_PATH,
          POST,
          user.userId,
        ),
      )

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(applicationRead)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicDeleteApplication(applicationId: String): Response {
    val user: AuthenticatedUser = currentUserService.getCurrentUser()

    // process and monitor the request
    val application: Application? =
      trackingHelper.callWithTracker(
        {
          applicationService.deleteApplication(user, applicationId)
        },
        APPLICATIONS_PATH_WITH_ID,
        DELETE,
        user.userId,
      )

    if (application == null) {
      throw ResourceNotFoundProblem(
        detail = "The application with the provided id was not found.",
        data = null,
      )
    }

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(
        toApplicationRead(application),
      ).build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Secured(SecurityRule.IS_ANONYMOUS)
  override fun publicGetAccessToken(applicationTokenRequestWithGrant: ApplicationTokenRequestWithGrant): Response =
    Response
      .status(Response.Status.OK.statusCode)
      .entity(
        PublicAccessTokenResponse(
          accessToken =
            applicationService
              .getToken(
                clientId = applicationTokenRequestWithGrant.clientId,
                clientSecret = applicationTokenRequestWithGrant.clientSecret,
              ),
          PublicAccessTokenResponse.TokenType.BEARER,
          tokenExpirationConfig.applicationTokenExpirationInMinutes.minutes.inWholeSeconds,
        ),
      ).build()

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicGetApplication(applicationId: String): Response {
    val user: AuthenticatedUser = currentUserService.getCurrentUser()

    // process and monitor the request
    val application: Optional<ApplicationRead> =
      trackingHelper.callWithTracker(
        {
          applicationService
            .listApplicationsByUser(user)
            .firstOrNull { app -> app.id == applicationId }
            ?.let { app -> toApplicationRead(app) }
            .let { Optional.ofNullable(it) }
        },
        APPLICATIONS_PATH_WITH_ID,
        GET,
        user.userId,
      )

    if (application.isEmpty) {
      throw ResourceNotFoundProblem(
        detail = "The application with the provided id was not found.",
        data = null,
      )
    }

    return Response
      .status(Response.Status.OK)
      .entity(application.get())
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListApplications(): Response {
    val user: AuthenticatedUser = currentUserService.getCurrentUser()

    // process and monitor the request
    val applications =
      trackingHelper.callWithTracker(
        {
          applicationService
            .listApplicationsByUser(user)
            .map { app -> toApplicationRead(app) }
        },
        APPLICATIONS_PATH,
        GET,
        user.userId,
      )

    return Response
      .status(Response.Status.OK)
      .entity(
        ApplicationReadList(
          applications = applications,
        ),
      ).build()
  }
}
