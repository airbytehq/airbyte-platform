/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionIdRequestBody;
import io.airbyte.api.model.generated.PermissionRead;
import io.airbyte.api.model.generated.PermissionReadList;
import io.airbyte.api.model.generated.PermissionUpdate;
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class PermissionApiControllerTest extends BaseControllerTest {

  @Test
  void testCreatePermission() throws IOException, JsonValidationException {
    Mockito.when(permissionHandler.createPermission(Mockito.any()))
        .thenReturn(new PermissionRead());
    final String path = "/api/v1/permissions/create";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionCreate().workspaceId(UUID.randomUUID()))),
        HttpStatus.OK);
  }

  @Test
  void testGetPermission() throws ConfigNotFoundException, IOException {
    Mockito.when(permissionHandler.getPermission(Mockito.any()))
        .thenReturn(new PermissionRead());
    final String path = "/api/v1/permissions/get";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testUpdatePermission() throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID userId = UUID.randomUUID();
    Mockito.when(permissionHandler.getPermission(Mockito.any()))
        .thenReturn(new PermissionRead().userId(userId));
    Mockito.when(permissionHandler.updatePermission(Mockito.any()))
        .thenReturn(new PermissionRead().userId(userId));
    final String path = "/api/v1/permissions/update";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionUpdate().permissionId(UUID.randomUUID()))),
        HttpStatus.OK);
  }

  @Test
  void testDeletePermission() throws IOException {
    Mockito.doNothing().when(permissionHandler).deletePermission(Mockito.any());
    final String path = "/api/v1/permissions/delete";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testListPermissionByUser() throws IOException {
    Mockito.when(permissionHandler.listPermissionsByUser(Mockito.any()))
        .thenReturn(new PermissionReadList());
    final String path = "/api/v1/permissions/list_by_user";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testCheckPermission() throws IOException {
    Mockito.when(permissionHandler.checkPermissions(Mockito.any()))
        .thenReturn(new PermissionCheckRead());
    final String path = "/api/v1/permissions/check";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionCheckRequest())),
        HttpStatus.OK);
  }

  @Test
  void testCheckMultipleWorkspacesPermission() throws IOException {
    Mockito.when(permissionHandler.permissionsCheckMultipleWorkspaces(Mockito.any()))
        .thenReturn(new PermissionCheckRead());
    final String path = "/api/v1/permissions/check_multiple_workspaces";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new PermissionsCheckMultipleWorkspacesRequest())),
        HttpStatus.OK);
  }

}
