/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.SELF;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.PermissionApi;
import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionDeleteUserFromWorkspaceRequestBody;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionReadList;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.commons.auth.SecuredUser;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

/**
 * This class is migrated from cloud-server PermissionApiController
 * {@link io.airbyte.cloud.server.apis.PermissionApiController}.
 *
 * TODO: migrate all Permission endpoints (including some endpoints in WebBackend API) from Cloud to
 * OSS.
 */
@Controller("/api/v1/permissions")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
public class PermissionApiController implements PermissionApi {

  private final PermissionHandler permissionHandler;

  public PermissionApiController(final PermissionHandler permissionHandler) {
    this.permissionHandler = permissionHandler;
  }

  @Secured({ORGANIZATION_ADMIN, WORKSPACE_ADMIN})
  @Post("/create")
  @Override
  public PermissionRead createPermission(final PermissionCreate permissionCreate) {
    return ApiHelper.execute(() -> {
      validatePermissionCreation(permissionCreate);
      return permissionHandler.createPermission(permissionCreate);
    });
  }

  private void validatePermissionCreation(final PermissionCreate permissionCreate) throws JsonValidationException {
    if (permissionCreate.getPermissionType() == PermissionType.INSTANCE_ADMIN) {
      throw new JsonValidationException("Instance Admin permissions cannot be created via API.");
    }
    if (permissionCreate.getOrganizationId() == null && permissionCreate.getWorkspaceId() == null) {
      throw new JsonValidationException("Either workspaceId or organizationId should be provided.");
    }
  }

  @Secured({ORGANIZATION_READER, WORKSPACE_READER})
  @Post("/get")
  @Override
  public PermissionRead getPermission(final PermissionIdRequestBody permissionIdRequestBody) {
    return ApiHelper.execute(() -> permissionHandler.getPermission(permissionIdRequestBody));
  }

  @Secured({ORGANIZATION_ADMIN, WORKSPACE_ADMIN})
  @Post("/update")
  @Override
  public PermissionRead updatePermission(final PermissionUpdate permissionUpdate) {
    return ApiHelper.execute(() -> {
      validatePermissionUpdate(permissionUpdate);
      return permissionHandler.updatePermission(permissionUpdate);
    });
  }

  private void validatePermissionUpdate(final PermissionUpdate permissionUpdate) throws JsonValidationException {
    if (permissionUpdate.getPermissionType() == PermissionType.INSTANCE_ADMIN) {
      throw new JsonValidationException("Cannot modify Instance Admin permissions via API.");
    }
  }

  @Secured({ORGANIZATION_ADMIN, WORKSPACE_ADMIN})
  @Post("/delete")
  @Override
  public void deletePermission(final PermissionIdRequestBody permissionIdRequestBody) {

    ApiHelper.execute(() -> {
      permissionHandler.deletePermission(permissionIdRequestBody);
      return null;
    });
  }

  @Secured({ORGANIZATION_ADMIN, WORKSPACE_ADMIN})
  @Post("/delete_user_from_workspace")
  @Override
  public void deleteUserFromWorkspace(final PermissionDeleteUserFromWorkspaceRequestBody permissionDeleteUserFromWorkspaceRequestBody) {
    ApiHelper.execute(() -> {
      permissionHandler.deleteUserFromWorkspace(permissionDeleteUserFromWorkspaceRequestBody);
      return null;
    });
  }

  @SecuredUser
  @Secured({ADMIN, SELF})
  @Post("/list_by_user")
  @Override
  public PermissionReadList listPermissionsByUser(final UserIdRequestBody userIdRequestBody) {
    return ApiHelper.execute(() -> permissionHandler.listPermissionsByUser(userIdRequestBody.getUserId()));
  }

  @Secured({ADMIN}) // instance admins only
  @Post("/check")
  @Override
  public PermissionCheckRead checkPermissions(final PermissionCheckRequest permissionCheckRequest) {

    return ApiHelper.execute(() -> permissionHandler.checkPermissions(permissionCheckRequest));
  }

  @Secured({ADMIN}) // instance admins only
  @Post("/check_multiple_workspaces")
  @Override
  public PermissionCheckRead checkPermissionsAcrossMultipleWorkspaces(final PermissionsCheckMultipleWorkspacesRequest request) {
    return ApiHelper.execute(() -> permissionHandler.permissionsCheckMultipleWorkspaces(request));
  }

}
