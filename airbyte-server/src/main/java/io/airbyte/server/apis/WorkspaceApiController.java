/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.WorkspaceApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.SlugRequestBody;
import io.airbyte.api.model.generated.WorkspaceCreate;
import io.airbyte.api.model.generated.WorkspaceGiveFeedback;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.api.model.generated.WorkspaceUpdateName;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.WorkspacesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/workspaces")
@Secured(SecurityRule.IS_AUTHENTICATED)
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "MissingJavadocType"})
public class WorkspaceApiController implements WorkspaceApi {

  private final WorkspacesHandler workspacesHandler;

  public WorkspaceApiController(final WorkspacesHandler workspacesHandler) {
    this.workspacesHandler = workspacesHandler;
  }

  @Post("/create")
  @Secured({AUTHENTICATED_USER})
  @Override
  public WorkspaceRead createWorkspace(@Body final WorkspaceCreate workspaceCreate) {
    return ApiHelper.execute(() -> workspacesHandler.createWorkspace(workspaceCreate));
  }

  @Post("/delete")
  @Secured({EDITOR})
  @SecuredWorkspace
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void deleteWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    ApiHelper.execute(() -> {
      workspacesHandler.deleteWorkspace(workspaceIdRequestBody);
      return null;
    });
  }

  @Post("/get")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspace(workspaceIdRequestBody));
  }

  @Post("/get_by_slug")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspaceBySlug(@Body final SlugRequestBody slugRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceBySlug(slugRequestBody));
  }

  @Post("/list")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listWorkspaces() {
    return ApiHelper.execute(workspacesHandler::listWorkspaces);
  }

  @Post("/list_all_paginated")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listAllWorkspacesPaginated(@Body final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.listAllWorkspacesPaginated(listResourcesForWorkspacesRequestBody));
  }

  @Post(uri = "/list_paginated")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listWorkspacesPaginated(@Body final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesPaginated(listResourcesForWorkspacesRequestBody));
  }

  @Post("/update")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead updateWorkspace(@Body final WorkspaceUpdate workspaceUpdate) {
    return ApiHelper.execute(() -> workspacesHandler.updateWorkspace(workspaceUpdate));
  }

  @Post("/tag_feedback_status_as_done")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public void updateWorkspaceFeedback(@Body final WorkspaceGiveFeedback workspaceGiveFeedback) {
    ApiHelper.execute(() -> {
      workspacesHandler.setFeedbackDone(workspaceGiveFeedback);
      return null;
    });
  }

  @Post("/update_name")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead updateWorkspaceName(@Body final WorkspaceUpdateName workspaceUpdateName) {
    return ApiHelper.execute(() -> workspacesHandler.updateWorkspaceName(workspaceUpdateName));
  }

  @Post("/get_by_connection_id")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspaceByConnectionId(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody));
  }

  @Override
  public WorkspaceReadList listWorkspacesInOrganization(@Body final ListWorkspacesInOrganizationRequestBody request) {
    // To be implemented
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesInOrganization(request));
  }

  @Post("/list_by_user_id")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listWorkspacesByUser(@Body final ListWorkspacesByUserRequestBody request) {
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesByUser(request));
  }

}
