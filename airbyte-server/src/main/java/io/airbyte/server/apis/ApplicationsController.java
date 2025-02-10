/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.micronaut.security.rules.SecurityRule.IS_ANONYMOUS;

import io.airbyte.api.generated.ApplicationsApi;
import io.airbyte.api.model.generated.AccessToken;
import io.airbyte.api.model.generated.ApplicationCreate;
import io.airbyte.api.model.generated.ApplicationIdRequestBody;
import io.airbyte.api.model.generated.ApplicationRead;
import io.airbyte.api.model.generated.ApplicationReadList;
import io.airbyte.api.model.generated.ApplicationTokenRequest;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.Application;
import io.airbyte.data.services.ApplicationService;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * An API for Applications. Tied to a user or non-user entity i.e. an organization.
 */
@Context
@Controller("/api/v1/applications")
@Requires(bean = ApplicationService.class)
public class ApplicationsController implements ApplicationsApi {

  final ApplicationService applicationService;
  final CurrentUserService currentUserService;

  public ApplicationsController(ApplicationService applicationService, CurrentUserService currentUserService) {
    this.applicationService = applicationService;
    this.currentUserService = currentUserService;
  }

  /**
   * Allows for a pass through that can be used to get a token.
   *
   * @param applicationTokenRequest The request to create the access token.
   * @return The created access token.
   */
  @Override
  @Secured(IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public AccessToken applicationTokenRequest(@Body ApplicationTokenRequest applicationTokenRequest) {
    final var token = applicationService.getToken(
        applicationTokenRequest.getClientId(),
        applicationTokenRequest.getClientSecret());

    final var accessToken = new AccessToken();
    accessToken.setAccessToken(token);
    return accessToken;
  }

  /**
   * Creates a new Application for a user.
   *
   * @param applicationCreate The Application request.
   * @return The created Application.
   */
  @Override
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ApplicationRead createApplication(@Body ApplicationCreate applicationCreate) {
    final var application = applicationService.createApplication(currentUserService.getCurrentUser(), applicationCreate.getName());
    return toApplicationRead(application);
  }

  /**
   * Deletes an Application for a user.
   *
   * @param applicationIdRequestBody The id of the Application to delete.
   */
  @Override
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void deleteApplication(@Body ApplicationIdRequestBody applicationIdRequestBody) {
    applicationService.deleteApplication(
        currentUserService.getCurrentUser(),
        applicationIdRequestBody.getApplicationId().toString());
  }

  /**
   * Lists all Applications for a user.
   *
   * @return The list of Applications.
   */
  @Override
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ApplicationReadList listApplications() {
    final var applications = applicationService.listApplicationsByUser(currentUserService.getCurrentUser())
        .stream()
        .map(this::toApplicationRead)
        .toList();

    final var applicationReadList = new ApplicationReadList();
    applicationReadList.setApplications(applications);
    return applicationReadList;
  }

  /**
   * Converts an Application to an ApplicationRead.
   *
   * @param application The Application to convert.
   * @return The converted Application.
   */
  private ApplicationRead toApplicationRead(Application application) {
    final var applicationRead = new ApplicationRead();
    applicationRead.setId(application.getId());
    applicationRead.setName(application.getName());
    applicationRead.setClientId(application.getClientId());
    applicationRead.setClientSecret(application.getClientSecret());
    applicationRead.setCreatedAt(
        OffsetDateTime
            .parse(application.getCreatedOn(), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .toEpochSecond());
    return applicationRead;
  }

}
