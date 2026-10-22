/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ApplicationsApi
import io.airbyte.api.server.generated.models.AccessToken
import io.airbyte.api.server.generated.models.ApplicationCreate
import io.airbyte.api.server.generated.models.ApplicationIdRequestBody
import io.airbyte.api.server.generated.models.ApplicationRead
import io.airbyte.api.server.generated.models.ApplicationReadList
import io.airbyte.api.server.generated.models.ApplicationTokenRequest
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Application
import io.airbyte.data.services.ApplicationService
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.annotations.RequestTimeout
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * An API for Applications. Tied to a user or non-user entity i.e. an organization.
 */
@Context
@Controller
@Requires(bean = ApplicationService::class)
open class ApplicationsController(
  val applicationService: ApplicationService,
  val currentUserService: CurrentUserService,
) : ApplicationsApi {
  /**
   * Allows for a pass through that can be used to get a token.
   *
   * @param applicationTokenRequest The request to create the access token.
   * @return The created access token.
   */
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @RequestTimeout(timeout = "PT1S")
  @AuthorizeOperation(
    access = RouteAccessPolicy.LOGIN,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun applicationTokenRequest(
    @Body applicationTokenRequest: ApplicationTokenRequest,
  ): AccessToken {
    ApmTraceUtils.addTagsToTrace(mapOf(MetricTags.CLIENT_ID to applicationTokenRequest.clientId))
    val token =
      applicationService.getToken(
        applicationTokenRequest.clientId,
        applicationTokenRequest.clientSecret,
      )

    return AccessToken(accessToken = token)
  }

  /**
   * Creates a new Application for a user.
   *
   * @param applicationCreate The Application request.
   * @return The created Application.
   */
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createApplication(
    @Body applicationCreate: ApplicationCreate,
  ): ApplicationRead {
    val application = applicationService.createApplication(currentUserService.getCurrentUser(), applicationCreate.name)
    return toApplicationRead(application)
  }

  /**
   * Deletes an Application for a user.
   *
   * @param applicationIdRequestBody The id of the Application to delete.
   */
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteApplication(
    @Body applicationIdRequestBody: ApplicationIdRequestBody,
  ) {
    applicationService.deleteApplication(
      currentUserService.getCurrentUser(),
      applicationIdRequestBody.applicationId.toString(),
    )
  }

  /**
   * Lists all Applications for a user.
   *
   * @return The list of Applications.
   */
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listApplications(): ApplicationReadList {
    val applications =
      applicationService
        .listApplicationsByUser(currentUserService.getCurrentUser())
        .map { application -> this.toApplicationRead(application) }

    return ApplicationReadList(applications = applications)
  }

  /**
   * Converts an Application to an ApplicationRead.
   *
   * @param application The Application to convert.
   * @return The converted Application.
   */
  private fun toApplicationRead(application: Application): ApplicationRead =
    ApplicationRead(
      id = application.id,
      name = application.name,
      clientId = application.clientId,
      clientSecret = application.clientSecret,
      createdAt =
        OffsetDateTime
          .parse(application.createdOn, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .toEpochSecond(),
    )
}
