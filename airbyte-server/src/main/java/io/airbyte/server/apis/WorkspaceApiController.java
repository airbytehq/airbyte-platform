/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.SELF;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.WorkspaceApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum;
import io.airbyte.api.model.generated.PermissionCheckRequest;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.SlugRequestBody;
import io.airbyte.api.model.generated.WorkspaceCreate;
import io.airbyte.api.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.model.generated.WorkspaceGiveFeedback;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceOrganizationInfoRead;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.api.model.generated.WorkspaceUpdateName;
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization;
import io.airbyte.api.model.generated.WorkspaceUsageRead;
import io.airbyte.api.model.generated.WorkspaceUsageRequestBody;
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem;
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.commons.server.handlers.WorkspacesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.server.support.CurrentUserService;
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
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class WorkspaceApiController implements WorkspaceApi {

  private final WorkspacesHandler workspacesHandler;
  private final PermissionHandler permissionHandler;
  private final CurrentUserService currentUserService;

  public WorkspaceApiController(final WorkspacesHandler workspacesHandler,
                                final PermissionHandler permissionHandler,
                                final CurrentUserService currentUserService) {
    this.workspacesHandler = workspacesHandler;
    this.permissionHandler = permissionHandler;
    this.currentUserService = currentUserService;
  }

  @Post("/create")
  @Secured({AUTHENTICATED_USER})
  @Override
  public WorkspaceRead createWorkspace(@Body final WorkspaceCreate workspaceCreate) {
    return ApiHelper.execute(() -> {
      // Verify that the user has permission to create a workspace in an organization,
      // need to be at least an organization admin to do so.
      if (workspaceCreate.getOrganizationId() != null) {
        final StatusEnum permissionCheckStatus = permissionHandler.checkPermissions(new PermissionCheckRequest()
            .userId(currentUserService.getCurrentUser().getUserId())
            .permissionType(PermissionType.ORGANIZATION_ADMIN)
            .organizationId(workspaceCreate.getOrganizationId()))
            .getStatus();
        if (!permissionCheckStatus.equals(StatusEnum.SUCCEEDED)) {
          throw new ForbiddenProblem(new ProblemMessageData()
              .message("User does not have permission to create a workspace in organization " + workspaceCreate.getOrganizationId()));
        }
      }
      return workspacesHandler.createWorkspace(workspaceCreate);
    });
  }

  @Post("/create_if_not_exist")
  @Secured({AUTHENTICATED_USER})
  @Override
  public WorkspaceRead createWorkspaceIfNotExist(@Body final WorkspaceCreateWithId workspaceCreateWithId) {
    return ApiHelper.execute(() -> {
      // Verify that the user has permission to create a workspace in an organization,
      // need to be at least an organization admin to do so.
      if (workspaceCreateWithId.getOrganizationId() != null) {
        final StatusEnum permissionCheckStatus = permissionHandler.checkPermissions(new PermissionCheckRequest()
            .userId(currentUserService.getCurrentUser().getUserId())
            .permissionType(PermissionType.ORGANIZATION_ADMIN)
            .organizationId(workspaceCreateWithId.getOrganizationId()))
            .getStatus();
        if (!permissionCheckStatus.equals(StatusEnum.SUCCEEDED)) {
          throw new ForbiddenProblem(new ProblemMessageData().message(
              "User does not have permission to create a workspace in organization " + workspaceCreateWithId.getOrganizationId()));
        }
      }
      return workspacesHandler.createWorkspaceIfNotExist(workspaceCreateWithId);
    });
  }

  @Post("/delete")
  @Secured({WORKSPACE_ADMIN, ORGANIZATION_ADMIN})
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void deleteWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    ApiHelper.execute(() -> {
      workspacesHandler.deleteWorkspace(workspaceIdRequestBody);
      return null;
    });
  }

  @Post("/get_organization_info")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceOrganizationInfoRead getOrganizationInfo(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceOrganizationInfo(workspaceIdRequestBody));
  }

  @Post("/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspace(workspaceIdRequestBody));
  }

  @Post("/get_by_slug")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspaceBySlug(@Body final SlugRequestBody slugRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceBySlug(slugRequestBody));
  }

  @Post("/get_usage")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceUsageRead getWorkspaceUsage(@Body final WorkspaceUsageRequestBody workspaceUsageRequestBody) {
    throw new ApiNotImplementedInOssProblem("Not implemented in this edition of Airbyte", null);
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
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listWorkspacesPaginated(@Body final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesPaginated(listResourcesForWorkspacesRequestBody));
  }

  @Post("/update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead updateWorkspace(@Body final WorkspaceUpdate workspaceUpdate) {
    return ApiHelper.execute(() -> workspacesHandler.updateWorkspace(workspaceUpdate));
  }

  @Post("/tag_feedback_status_as_done")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public void updateWorkspaceFeedback(@Body final WorkspaceGiveFeedback workspaceGiveFeedback) {
    ApiHelper.execute(() -> {
      workspacesHandler.setFeedbackDone(workspaceGiveFeedback);
      return null;
    });
  }

  @Post("/update_name")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead updateWorkspaceName(@Body final WorkspaceUpdateName workspaceUpdateName) {
    return ApiHelper.execute(() -> workspacesHandler.updateWorkspaceName(workspaceUpdateName));
  }

  @Post("/update_organization")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead updateWorkspaceOrganization(@Body final WorkspaceUpdateOrganization workspaceUpdateOrganization) {
    return ApiHelper.execute(() -> workspacesHandler.updateWorkspaceOrganization(workspaceUpdateOrganization));
  }

  @Post("/get_by_connection_id")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspaceByConnectionId(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, false));
  }

  @Post("/get_by_connection_id_with_tombstone")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceRead getWorkspaceByConnectionIdWithTombstone(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, true));
  }

  @Override
  public WorkspaceReadList listWorkspacesInOrganization(@Body final ListWorkspacesInOrganizationRequestBody request) {
    // To be implemented
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesInOrganization(request));
  }

  @Post("/list_by_user_id")
  @Secured({READER, SELF})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WorkspaceReadList listWorkspacesByUser(@Body final ListWorkspacesByUserRequestBody request) {
    return ApiHelper.execute(() -> workspacesHandler.listWorkspacesByUser(request));
  }

}
