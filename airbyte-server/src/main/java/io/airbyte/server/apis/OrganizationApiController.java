/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_MEMBER;
import static io.airbyte.commons.auth.AuthRoleConstants.SELF;

import io.airbyte.api.generated.OrganizationApi;
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody;
import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationReadList;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.api.model.generated.OrganizationUsageRead;
import io.airbyte.api.model.generated.OrganizationUsageRequestBody;
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem;
import io.airbyte.commons.auth.generated.Intent;
import io.airbyte.commons.auth.permissions.RequiresIntent;
import io.airbyte.commons.server.handlers.OrganizationsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@SuppressWarnings("ParameterName")
@Controller("/api/v1/organizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class OrganizationApiController implements OrganizationApi {

  private final OrganizationsHandler organizationsHandler;

  public OrganizationApiController(final OrganizationsHandler organizationsHandler) {
    this.organizationsHandler = organizationsHandler;
  }

  @Post("/get")
  @Secured({ORGANIZATION_MEMBER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationRead getOrganization(@Body final OrganizationIdRequestBody organizationIdRequestBody) {
    return ApiHelper.execute(() -> organizationsHandler.getOrganization(organizationIdRequestBody));
  }

  @Post("/update")
  @Secured({ORGANIZATION_EDITOR})
  @Override
  public OrganizationRead updateOrganization(@Body final OrganizationUpdateRequestBody organizationUpdateRequestBody) {
    return ApiHelper.execute(() -> organizationsHandler.updateOrganization(organizationUpdateRequestBody));
  }

  @Post("/create")
  @Secured({ADMIN}) // instance admin only
  @Override
  public OrganizationRead createOrganization(@Body final OrganizationCreateRequestBody organizationCreateRequestBody) {
    return ApiHelper.execute(() -> organizationsHandler.createOrganization(organizationCreateRequestBody));
  }

  @Post("/delete")
  @Secured({ADMIN}) // instance admin only
  @Override
  public void deleteOrganization(@Body final OrganizationIdRequestBody organizationIdRequestBody) {
    // To be implemented; we need a tombstone column for organizations table.
  }

  @Post("/list_by_user_id")
  @Secured({ADMIN, SELF})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationReadList listOrganizationsByUser(@Body final ListOrganizationsByUserRequestBody request) {
    return ApiHelper.execute(() -> organizationsHandler.listOrganizationsByUser(request));
  }

  @Post("/get_usage")
  @RequiresIntent(Intent.ViewOrganizationUsage)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationUsageRead getOrganizationUsage(@Body OrganizationUsageRequestBody organizationUsageRequestBody) {
    throw new ApiNotImplementedInOssProblem();
  }

}
