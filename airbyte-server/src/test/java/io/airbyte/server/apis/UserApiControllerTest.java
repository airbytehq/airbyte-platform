/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationUserReadList;
import io.airbyte.api.model.generated.UserAuthIdRequestBody;
import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserEmailRequestBody;
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse;
import io.airbyte.api.model.generated.UserIdRequestBody;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserUpdate;
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceUserReadList;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class UserApiControllerTest extends BaseControllerTest {

  @Test
  void testCreateUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(userHandler.createUser(Mockito.any()))
        .thenReturn(new UserRead());
    final String path = "/api/v1/users/create";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserCreate())),
        HttpStatus.OK);
  }

  @Test
  void testGetUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(userHandler.getUser(Mockito.any()))
        .thenReturn(new UserRead());
    final String path = "/api/v1/users/get";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testGetUserByAuthId() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(userHandler.getUserByAuthId(Mockito.any()))
        .thenReturn(new UserRead());
    final String path = "/api/v1/users/get_by_auth_id";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserAuthIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testGetUserByEmail() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(userHandler.getUserByEmail(Mockito.any()))
        .thenReturn(new UserRead());
    final String path = "/api/v1/users/get_by_email";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserEmailRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testDeleteUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.doNothing().when(userHandler).deleteUser(Mockito.any());
    final String path = "/api/v1/users/delete";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testUpdateUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(userHandler.updateUser(Mockito.any()))
        .thenReturn(new UserRead());
    final String path = "/api/v1/users/update";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserUpdate())),
        HttpStatus.OK);
  }

  @Test
  void testListUsersInOrganization() throws IOException, ConfigNotFoundException {
    Mockito.when(userHandler.listUsersInOrganization(Mockito.any()))
        .thenReturn(new OrganizationUserReadList());
    final String path = "/api/v1/users/list_by_organization_id";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new OrganizationIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testListUsersInWorkspace() throws Exception {
    Mockito.when(userHandler.listUsersInWorkspace(Mockito.any()))
        .thenReturn(new WorkspaceUserReadList());
    final String path = "/api/v1/users/list_by_workspace_id";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new WorkspaceIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testListInstanceAdminUsers() throws Exception {
    Mockito.when(userHandler.listInstanceAdminUsers())
        .thenReturn(new UserWithPermissionInfoReadList());
    final String path = "/api/v1/users/list_instance_admin";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.emptyObject()),
        HttpStatus.OK);
  }

  @Test
  void testGetOrCreateUser() throws Exception {
    Mockito.when(userHandler.getOrCreateUserByAuthId(Mockito.any()))
        .thenReturn(new UserGetOrCreateByAuthIdResponse().userRead(new UserRead()));
    final String path = "/api/v1/users/get_or_create_by_auth_id";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new UserAuthIdRequestBody())),
        HttpStatus.OK);
  }

}
