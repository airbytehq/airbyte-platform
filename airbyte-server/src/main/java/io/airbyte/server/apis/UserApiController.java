/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_MEMBER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.SELF;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.UserApi;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserEmailRequestBody;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserUpdate;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
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
@Controller("/api/v1/users")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class UserApiController implements UserApi {

  private final UserHandler userHandler;

  public UserApiController(final UserHandler userHandler) {
    this.userHandler = userHandler;
  }

  @Post("/get")
  @Secured({ADMIN, SELF})
  @Override
  public UserRead getUser(@Body final UserIdRequestBody userIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.getUser(userIdRequestBody));
  }

  @Post("/get_by_auth_id")
  @Secured({ADMIN, SELF})
  @Override
  public UserRead getUserByAuthId(@Body final UserAuthIdRequestBody userAuthIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.getUserByAuthId(userAuthIdRequestBody));
  }

  @Post("/get_by_email")
  @Secured({ADMIN, SELF})
  @Override
  public UserRead getUserByEmail(@Body final UserEmailRequestBody userEmailRequestBody) {
    return ApiHelper.execute(() -> userHandler.getUserByEmail(userEmailRequestBody));
  }

  @Post("/delete")
  @Secured({ADMIN, SELF})
  @Override
  public void deleteUser(@Body final UserIdRequestBody userIdRequestBody) {
    ApiHelper.execute(
        () -> {
          userHandler.deleteUser(userIdRequestBody);
          return null;
        });
  }

  @Post("/update")
  @Secured({ADMIN, SELF})
  @Override
  public UserRead updateUser(@Body final UserUpdate userUpdate) {
    return ApiHelper.execute(() -> userHandler.updateUser(userUpdate));
  }

  @Post("/list_by_organization_id")
  @Secured({ORGANIZATION_MEMBER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public OrganizationUserReadList listUsersInOrganization(@Body final OrganizationIdRequestBody organizationIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.listUsersInOrganization(organizationIdRequestBody));
  }

  @Post("/list_by_workspace_id")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceUserReadList listUsersInWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.listUsersInWorkspace(workspaceIdRequestBody));
  }

  @Post("/list_access_info_by_workspace_id")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceUserAccessInfoReadList listAccessInfoByWorkspaceId(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.listAccessInfoByWorkspaceId(workspaceIdRequestBody));
  }

  @Post("/list_instance_admins")
  @Secured({ADMIN}) // instance admin only
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public UserWithPermissionInfoReadList listInstanceAdminUsers() {
    return ApiHelper.execute(userHandler::listInstanceAdminUsers);
  }

  @Post("/get_or_create_by_auth_id")
  @Secured({ADMIN, SELF})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public UserGetOrCreateByAuthIdResponse getOrCreateUserByAuthId(@Body final UserAuthIdRequestBody userAuthIdRequestBody) {
    return ApiHelper.execute(() -> userHandler.getOrCreateUserByAuthId(userAuthIdRequestBody));
  }

}
