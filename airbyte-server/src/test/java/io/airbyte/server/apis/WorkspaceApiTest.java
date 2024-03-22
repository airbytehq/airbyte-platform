/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.ArgumentMatchers.anyBoolean;

import io.airbyte.api.model.generated.PermissionCheckRead;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceCreate;
import io.airbyte.api.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization;
import io.airbyte.config.User;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class WorkspaceApiTest extends BaseControllerTest {

  @Test
  void testCreateWorkspace() throws JsonValidationException, IOException, ConfigNotFoundException {
    Mockito.when(permissionHandler.checkPermissions(Mockito.any()))
        .thenReturn(new PermissionCheckRead().status(StatusEnum.SUCCEEDED)) // first call with an orgId succeeds
        .thenReturn(new PermissionCheckRead().status(StatusEnum.FAILED)); // second call with an orgId fails

    Mockito.when(workspacesHandler.createWorkspace(Mockito.any()))
        .thenReturn(new WorkspaceRead());

    Mockito.when(currentUserService.getCurrentUser()).thenReturn(new User());

    final String path = "/api/v1/workspaces/create";

    // no org id, expect 200
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);

    // org id present, permission check succeeds, expect 200
    testEndpointStatus(
        HttpRequest.POST(path, new WorkspaceCreate().organizationId(UUID.randomUUID())),
        HttpStatus.OK);

    // org id present, permission check fails, expect 403
    testErrorEndpointStatus(
        HttpRequest.POST(path, new WorkspaceCreate().organizationId(UUID.randomUUID())),
        HttpStatus.FORBIDDEN);
  }

  @Test
  void testCreateWorkspaceIfNotExist() throws JsonValidationException, IOException, ConfigNotFoundException {
    Mockito.when(permissionHandler.checkPermissions(Mockito.any()))
        .thenReturn(new PermissionCheckRead().status(StatusEnum.SUCCEEDED)) // first call with an orgId succeeds
        .thenReturn(new PermissionCheckRead().status(StatusEnum.FAILED)); // second call with an orgId fails

    Mockito.when(workspacesHandler.createWorkspaceIfNotExist(Mockito.any()))
        .thenReturn(new WorkspaceRead());

    Mockito.when(currentUserService.getCurrentUser()).thenReturn(new User());

    final String path = "/api/v1/workspaces/create_if_not_exist";

    // no org id, expect 200
    testEndpointStatus(
        HttpRequest.POST(path, new WorkspaceCreateWithId()),
        HttpStatus.OK);

    // org id present, permission check succeeds, expect 200
    testEndpointStatus(
        HttpRequest.POST(path, new WorkspaceCreateWithId().organizationId(UUID.randomUUID())),
        HttpStatus.OK);

    // org id present, permission check fails, expect 403
    testErrorEndpointStatus(
        HttpRequest.POST(path, new WorkspaceCreateWithId().organizationId(UUID.randomUUID())),
        HttpStatus.FORBIDDEN);
  }

  @Test
  void testDeleteWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.doNothing()
        .doThrow(new ConfigNotFoundException("", ""))
        .when(workspacesHandler).deleteWorkspace(Mockito.any());
    final String path = "/api/v1/workspaces/delete";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.NO_CONTENT);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(workspacesHandler.getWorkspace(Mockito.any()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/get";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetBySlugWorkspace() throws ConfigNotFoundException, IOException {
    Mockito.when(workspacesHandler.getWorkspaceBySlug(Mockito.any()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/get_by_slug";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testListWorkspace() throws JsonValidationException, IOException {
    Mockito.when(workspacesHandler.listWorkspaces())
        .thenReturn(new WorkspaceReadList());
    final String path = "/api/v1/workspaces/list";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testUpdateWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(workspacesHandler.updateWorkspace(Mockito.any()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/update";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testUpdateWorkspaceOrganization() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(workspacesHandler.updateWorkspaceOrganization(Mockito.any()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/update_organization";
    testEndpointStatus(
        HttpRequest.POST(path, new WorkspaceUpdateOrganization()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new WorkspaceUpdateOrganization()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testUpdateWorkspaceFeedback() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.doNothing()
        .doThrow(new ConfigNotFoundException("", ""))
        .when(workspacesHandler).setFeedbackDone(Mockito.any());
    final String path = "/api/v1/workspaces/tag_feedback_status_as_done";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testUpdateWorkspaceName() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(workspacesHandler.updateWorkspaceName(Mockito.any()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/update_name";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetWorkspaceByConnectionId() throws ConfigNotFoundException {
    Mockito.when(workspacesHandler.getWorkspaceByConnectionId(Mockito.any(), anyBoolean()))
        .thenReturn(new WorkspaceRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/workspaces/get_by_connection_id";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

}
