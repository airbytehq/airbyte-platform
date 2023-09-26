/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.OrganizationApi;
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody;
import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationReadList;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.commons.server.handlers.OrganizationsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@SuppressWarnings({"MissingJavadocType", "ParameterName"})
@Controller("/api/v1/organizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class OrganizationApiController implements OrganizationApi {

  private final OrganizationsHandler organizationsHandler;

  public OrganizationApiController(final OrganizationsHandler organizationsHandler) {
    this.organizationsHandler = organizationsHandler;
  }

  @Post("/get")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationRead getOrganization(OrganizationIdRequestBody organizationIdRequestBody) {
    // To be implemented
    return ApiHelper.execute(() -> organizationsHandler.getOrganization(organizationIdRequestBody));
  }

  @Override
  public OrganizationRead updateOrganization(OrganizationUpdateRequestBody organizationUpdateRequestBody) {
    return ApiHelper.execute(() -> organizationsHandler.updateOrganization(organizationUpdateRequestBody));
  }

  @Override
  public OrganizationRead createOrganization(OrganizationCreateRequestBody organizationCreateRequestBody) {
    return ApiHelper.execute(() -> organizationsHandler.createOrganization(organizationCreateRequestBody));
  }

  @Override
  public void deleteOrganization(OrganizationIdRequestBody organizationIdRequestBody) {
    // To be implemented; we need a tombstone column for organizations table.
  }

  @Post("/list_by_user_id")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationReadList listOrganizationsByUser(@Body final ListOrganizationsByUserRequestBody request) {
    return ApiHelper.execute(() -> organizationsHandler.listOrganizationsByUser(request));
  }

}
