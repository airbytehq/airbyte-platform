/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.UserApi;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserUpdate;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.commons.auth.SecuredUser;
import io.airbyte.commons.server.handlers.UserHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

/**
 * User related APIs. TODO: migrate all User endpoints (including some endpoints in WebBackend API)
 * from Cloud to OSS.
 */
@SuppressWarnings("MissingJavadocType")
@Controller("/api/v1/users")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class UserApiController implements UserApi {

  private final UserHandler userHandler;

  public UserApiController(final UserHandler userHandler) {
    this.userHandler = userHandler;
  }

  @Post("/create")
  @SecuredUser
  @Secured({ADMIN})
  @Override
  public UserRead createUser(final UserCreate userCreate) {
    return ApiHelper.execute(() -> userHandler.createUser(userCreate));
  }

  @Post("/get")
  @SecuredUser
  @Secured({ADMIN})
  @Override
  public UserRead getUser(final UserIdRequestBody userIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.getUser(userIdRequestBody));
  }

  @Post("/get_by_auth_id")
  @SecuredUser
  @Secured({ADMIN})
  @Override
  public UserRead getUserByAuthId(final UserAuthIdRequestBody userAuthIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.getUserByAuthId(userAuthIdRequestBody));
  }

  @Post("/delete")
  @SecuredUser
  @Secured({ADMIN})
  @Override
  public void deleteUser(final UserIdRequestBody userIdRequestBody) {
    ApiHelper.execute(
        () -> {
          userHandler.deleteUser(userIdRequestBody);
          return null;
        });
  }

  @Post("/update")
  @SecuredUser
  @Secured({ADMIN})
  @Override
  public UserRead updateUser(final UserUpdate userUpdate) {
    return ApiHelper.execute(() -> userHandler.updateUser(userUpdate));
  }

  @Post("/list_by_organization_id")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationUserReadList listUsersInOrganization(OrganizationIdRequestBody organizationIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.listUsersInOrganization(organizationIdRequestBody));
  }

  @Post("/list_by_workspace_id")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceUserReadList listUsersInWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.listUsersInWorkspace(workspaceIdRequestBody));
  }

  // TODO: Update permission to instance admin once the permission PR is merged.
  @Post("/list_instance_admins")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public UserWithPermissionInfoReadList listInstanceAdminUsers() {
    return ApiHelper.execute(() -> userHandler.listInstanceAdminUsers());
  }

}
