/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.PermissionApi;
import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionReadList;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
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
@Controller("api/v1/permissions")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
public class PermissionApiController implements PermissionApi {

  private final PermissionHandler permissionHandler;

  public PermissionApiController(final PermissionHandler permissionHandler) {
    this.permissionHandler = permissionHandler;
  }

  @SecuredWorkspace
  @Secured({ADMIN})
  @Post("/create")
  @Override
  public PermissionRead createPermission(final PermissionCreate permissionCreate) {
    return ApiHelper.execute(() -> permissionHandler.createPermission(permissionCreate));
  }

  @SecuredWorkspace
  @Secured({READER})
  @Post("/get")
  @Override
  public PermissionRead getPermission(final PermissionIdRequestBody permissionIdRequestBody) {
    return ApiHelper.execute(() -> permissionHandler.getPermission(permissionIdRequestBody));
  }

  @Secured({ADMIN})
  @Post("/update")
  @Override
  public PermissionRead updatePermission(final PermissionUpdate permissionUpdate) {
    // Admin users can update permission including permission type, access to which workspace or
    // organization.

    return ApiHelper.execute(() -> {

      final PermissionRead oldPermission =
          permissionHandler.getPermission(new PermissionIdRequestBody().permissionId(permissionUpdate.getPermissionId()));

      if (oldPermission != null && !oldPermission.getUserId().equals(permissionUpdate.getUserId())) {
        throw new IllegalArgumentException("The update can not change the user id!");
      }

      return permissionHandler.updatePermission(permissionUpdate);
    });
  }

  @SecuredWorkspace
  @Secured({ADMIN})
  @Post("/delete")
  @Override
  public void deletePermission(final PermissionIdRequestBody permissionIdRequestBody) {

    ApiHelper.execute(() -> {
      permissionHandler.deletePermission(permissionIdRequestBody);
      return null;
    });
  }

  @SecuredWorkspace
  @Secured({ADMIN})
  @Post("/list_by_user")
  @Override
  public PermissionReadList listPermissionsByUser(final UserIdRequestBody userIdRequestBody) {
    return ApiHelper.execute(() -> permissionHandler.listPermissionsByUser(userIdRequestBody.getUserId()));
  }

  @Secured({ADMIN})
  @Post("/check")
  @Override
  public PermissionCheckRead checkPermissions(final PermissionCheckRequest permissionCheckRequest) {

    return ApiHelper.execute(() -> permissionHandler.checkPermissions(permissionCheckRequest));
  }

  @Secured({ADMIN})
  @Post("/check_multiple_workspaces")
  @Override
  public PermissionCheckRead checkPermissionsAcrossMultipleWorkspaces(final PermissionsCheckMultipleWorkspacesRequest request) {
    return ApiHelper.execute(() -> permissionHandler.permissionsCheckMultipleWorkspaces(request));
  }

}
