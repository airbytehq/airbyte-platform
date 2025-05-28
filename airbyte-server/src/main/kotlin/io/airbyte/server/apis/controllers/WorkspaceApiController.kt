/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.WorkspaceApi
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.SlugRequestBody
import io.airbyte.api.model.generated.TimeWindowRequestBody
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceGetDbtJobsRequest
import io.airbyte.api.model.generated.WorkspaceGetDbtJobsResponse
import io.airbyte.api.model.generated.WorkspaceGiveFeedback
import io.airbyte.api.model.generated.WorkspaceIdList
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceOrganizationInfoRead
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.api.model.generated.WorkspaceUpdateName
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization
import io.airbyte.api.model.generated.WorkspaceUsageRead
import io.airbyte.api.model.generated.WorkspaceUsageRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.apis.execute
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/workspaces")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkspaceApiController(
  private val workspacesHandler: WorkspacesHandler,
  private val permissionHandler: PermissionHandler,
  private val currentUserService: CurrentUserService,
) : WorkspaceApi {
  @Post("/create")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createWorkspace(
    @Body workspaceCreate: WorkspaceCreate,
  ): WorkspaceRead? =
    execute {
      // Verify that the user has permission to create a workspace in an organization,
      // need to be at least an organization admin to do so.
      if (workspaceCreate.organizationId != null) {
        val permissionCheckStatus =
          permissionHandler
            .checkPermissions(
              PermissionCheckRequest()
                .userId(currentUserService.currentUser.userId)
                .permissionType(PermissionType.ORGANIZATION_ADMIN)
                .organizationId(workspaceCreate.organizationId),
            ).status
        if (permissionCheckStatus != PermissionCheckRead.StatusEnum.SUCCEEDED) {
          throw ForbiddenProblem(
            ProblemMessageData()
              .message("User does not have permission to create a workspace in organization " + workspaceCreate.organizationId),
          )
        }
      }
      workspacesHandler.createWorkspace(workspaceCreate)
    }

  @Post("/create_if_not_exist")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  override fun createWorkspaceIfNotExist(
    @Body workspaceCreateWithId: WorkspaceCreateWithId,
  ): WorkspaceRead? =
    execute {
      // Verify that the user has permission to create a workspace in an organization,
      // need to be at least an organization admin to do so.
      if (workspaceCreateWithId.organizationId != null) {
        val permissionCheckStatus =
          permissionHandler
            .checkPermissions(
              PermissionCheckRequest()
                .userId(currentUserService.currentUser.userId)
                .permissionType(PermissionType.ORGANIZATION_ADMIN)
                .organizationId(workspaceCreateWithId.organizationId),
            ).status
        if (permissionCheckStatus != PermissionCheckRead.StatusEnum.SUCCEEDED) {
          throw ForbiddenProblem(
            ProblemMessageData().message(
              "User does not have permission to create a workspace in organization " + workspaceCreateWithId.organizationId,
            ),
          )
        }
      }
      workspacesHandler.createWorkspaceIfNotExist(workspaceCreateWithId)
    }

  @Post("/delete")
  @Secured(AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.ORGANIZATION_ADMIN)
  @Status(HttpStatus.NO_CONTENT)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ) {
    execute<Any?> {
      workspacesHandler.deleteWorkspace(workspaceIdRequestBody)
      null
    }
  }

  @Post("/get_organization_info")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationInfo(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceOrganizationInfoRead? =
    execute {
      workspacesHandler.getWorkspaceOrganizationInfo(
        workspaceIdRequestBody,
      )
    }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceRead? = execute { workspacesHandler.getWorkspace(workspaceIdRequestBody) }

  @Post("/get_by_slug")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWorkspaceBySlug(
    @Body slugRequestBody: SlugRequestBody,
  ): WorkspaceRead? = execute { workspacesHandler.getWorkspaceBySlug(slugRequestBody) }

  @Post("/get_usage")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWorkspaceUsage(
    @Body workspaceUsageRequestBody: WorkspaceUsageRequestBody?,
  ): WorkspaceUsageRead = throw ApiNotImplementedInOssProblem("Not implemented in this edition of Airbyte", null)

  @Post("/list_all_paginated")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAllWorkspacesPaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): WorkspaceReadList? =
    execute {
      workspacesHandler.listAllWorkspacesPaginated(
        listResourcesForWorkspacesRequestBody,
      )
    }

  @Post(uri = "/list_paginated")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listWorkspacesPaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): WorkspaceReadList? =
    execute {
      workspacesHandler.listWorkspacesPaginated(
        listResourcesForWorkspacesRequestBody,
      )
    }

  @Post("/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateWorkspace(
    @Body workspaceUpdate: WorkspaceUpdate,
  ): WorkspaceRead? = execute { workspacesHandler.updateWorkspace(workspaceUpdate) }

  @Post("/tag_feedback_status_as_done")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateWorkspaceFeedback(
    @Body workspaceGiveFeedback: WorkspaceGiveFeedback,
  ) {
    execute<Any?> {
      workspacesHandler.setFeedbackDone(workspaceGiveFeedback)
      null
    }
  }

  @Post("/update_name")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateWorkspaceName(
    @Body workspaceUpdateName: WorkspaceUpdateName,
  ): WorkspaceRead? = execute { workspacesHandler.updateWorkspaceName(workspaceUpdateName) }

  @Post("/update_organization")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateWorkspaceOrganization(
    @Body workspaceUpdateOrganization: WorkspaceUpdateOrganization,
  ): WorkspaceRead? = execute { workspacesHandler.updateWorkspaceOrganization(workspaceUpdateOrganization) }

  @Post("/get_by_connection_id")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWorkspaceByConnectionId(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): WorkspaceRead? = execute { workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, false) }

  @Post("/get_by_connection_id_with_tombstone")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWorkspaceByConnectionIdWithTombstone(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): WorkspaceRead? = execute { workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, true) }

  override fun listWorkspacesInOrganization(
    @Body request: ListWorkspacesInOrganizationRequestBody,
  ): WorkspaceReadList? {
    // To be implemented
    return execute { workspacesHandler.listWorkspacesInOrganization(request) }
  }

  @Post("/list_by_user_id")
  @Secured(AuthRoleConstants.READER, AuthRoleConstants.SELF)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listWorkspacesByUser(
    @Body request: ListWorkspacesByUserRequestBody,
  ): WorkspaceReadList? = execute { workspacesHandler.listWorkspacesByUser(request) }

  @Post("/list_workspaces_by_most_recently_running_jobs")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listActiveWorkspacesByMostRecentlyRunningJobs(
    @Body timeWindowRequestBody: TimeWindowRequestBody,
  ): WorkspaceIdList = throw ApiNotImplementedInOssProblem()

  @Post("/get_available_dbt_jobs")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getAvailableDbtJobsForWorkspace(workspaceGetDbtJobsRequest: WorkspaceGetDbtJobsRequest?): WorkspaceGetDbtJobsResponse =
    throw ApiNotImplementedInOssProblem()
}
