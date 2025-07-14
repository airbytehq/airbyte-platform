/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ApplicationsApi
import io.airbyte.api.model.generated.AccessToken
import io.airbyte.api.model.generated.ApplicationCreate
import io.airbyte.api.model.generated.ApplicationIdRequestBody
import io.airbyte.api.model.generated.ApplicationRead
import io.airbyte.api.model.generated.ApplicationReadList
import io.airbyte.api.model.generated.ApplicationTokenRequest
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Application
import io.airbyte.data.services.ApplicationService
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
@Controller("/api/v1/applications")
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
  override fun applicationTokenRequest(
    @Body applicationTokenRequest: ApplicationTokenRequest,
  ): AccessToken {
    val token =
      applicationService.getToken(
        applicationTokenRequest.clientId,
        applicationTokenRequest.clientSecret,
      )

    val accessToken = AccessToken()
    accessToken.accessToken = token
    return accessToken
  }

  /**
   * Creates a new Application for a user.
   *
   * @param applicationCreate The Application request.
   * @return The created Application.
   */
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
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
  override fun listApplications(): ApplicationReadList? {
    val applications =
      applicationService
        .listApplicationsByUser(currentUserService.getCurrentUser())
        .map { application -> this.toApplicationRead(application) }

    val applicationReadList =
      io.airbyte.api.model.generated
        .ApplicationReadList()
    applicationReadList.setApplications(applications)
    return applicationReadList
  }

  /**
   * Converts an Application to an ApplicationRead.
   *
   * @param application The Application to convert.
   * @return The converted Application.
   */
  private fun toApplicationRead(application: Application): ApplicationRead {
    val applicationRead = ApplicationRead()
    applicationRead.id = application.id
    applicationRead.name = application.name
    applicationRead.clientId = application.clientId
    applicationRead.clientSecret = application.clientSecret
    applicationRead.createdAt =
      OffsetDateTime
        .parse(application.createdOn, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toEpochSecond()
    return applicationRead
  }
}
