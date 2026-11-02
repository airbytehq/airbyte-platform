/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.WorkspaceApi
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.ListWorkspacesByUserRequestBody
import io.airbyte.api.server.generated.models.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.server.generated.models.OrganizationInfoRead
import io.airbyte.api.server.generated.models.SlugRequestBody
import io.airbyte.api.server.generated.models.WorkspaceCreate
import io.airbyte.api.server.generated.models.WorkspaceCreateWithId
import io.airbyte.api.server.generated.models.WorkspaceDataWorkerAvailabilityRead
import io.airbyte.api.server.generated.models.WorkspaceDataWorkerAvailabilityRequestBody
import io.airbyte.api.server.generated.models.WorkspaceGetDbtJobsRequest
import io.airbyte.api.server.generated.models.WorkspaceGetDbtJobsResponse
import io.airbyte.api.server.generated.models.WorkspaceGiveFeedback
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceReadList
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.api.server.generated.models.WorkspaceUpdateName
import io.airbyte.api.server.generated.models.WorkspaceUpdateOrganization
import io.airbyte.api.server.generated.models.WorkspaceUsageRead
import io.airbyte.api.server.generated.models.WorkspaceUsageRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.OssUnsupportedOperationRule
import io.airbyte.server.auth.WorkspaceAdminRule
import io.airbyte.server.auth.WorkspaceCreateRule
import io.airbyte.server.auth.WorkspaceFilteredOrganizationRule
import io.airbyte.server.auth.WorkspaceGetRule
import io.airbyte.server.auth.WorkspaceListRule
import io.airbyte.server.auth.WorkspaceReadRule
import io.airbyte.server.auth.WorkspaceReadUnsupportedRule
import io.airbyte.server.auth.WorkspaceTombstoneConnectionRule
import io.airbyte.server.auth.WorkspaceTransferRule
import io.airbyte.server.auth.WorkspaceUserListRule
import io.airbyte.server.auth.WorkspaceWriteRule
import io.airbyte.server.auth.WorkspaceWriteUnsupportedRule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkspaceApiController(
  private val workspacesHandler: WorkspacesHandler,
  private val currentUserService: CurrentUserService,
) : WorkspaceApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = OssUnsupportedOperationRule::class,
  )
  override fun getWorkspaceSkillDocs(
    workspaceId: String,
    id: String,
    section: String?,
  ): JsonNode = throw ApiNotImplementedInOssProblem()

  @AuditLogging(provider = AuditLoggingProvider.WORKSPACE)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceCreateRule::class,
  )
  override fun createWorkspace(
    @Body workspaceCreate: WorkspaceCreate,
  ): WorkspaceRead = execute { workspacesHandler.createWorkspace(workspaceCreate) }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceCreateRule::class,
  )
  override fun createWorkspaceIfNotExist(
    @Body workspaceCreateWithId: WorkspaceCreateWithId,
  ): WorkspaceRead = execute { workspacesHandler.createWorkspaceIfNotExist(workspaceCreateWithId) }

  @Status(HttpStatus.NO_CONTENT)
  @AuditLogging(provider = AuditLoggingProvider.WORKSPACE)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceAdminRule::class,
  )
  override fun deleteWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ) {
    execute<Any?> {
      workspacesHandler.deleteWorkspace(workspaceIdRequestBody)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceReadRule::class,
  )
  override fun getOrganizationInfo(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): OrganizationInfoRead =
    execute {
      workspacesHandler.getWorkspaceOrganizationInfo(
        workspaceIdRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceGetRule::class,
  )
  override fun getWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WorkspaceRead = execute { workspacesHandler.getWorkspace(workspaceIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceReadRule::class,
  )
  override fun getWorkspaceBySlug(
    @Body slugRequestBody: SlugRequestBody,
  ): WorkspaceRead = execute { workspacesHandler.getWorkspaceBySlug(slugRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceReadUnsupportedRule::class,
  )
  override fun getWorkspaceUsage(
    @Body workspaceUsageRequestBody: WorkspaceUsageRequestBody?,
  ): WorkspaceUsageRead = throw ApiNotImplementedInOssProblem("Not implemented in this edition of Airbyte", null)

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceReadUnsupportedRule::class,
  )
  override fun getWorkspaceDataWorkerAvailability(
    @Body workspaceDataWorkerAvailabilityRequestBody: WorkspaceDataWorkerAvailabilityRequestBody?,
  ): WorkspaceDataWorkerAvailabilityRead = throw ApiNotImplementedInOssProblem("Not implemented in this edition of Airbyte", null)

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceListRule::class,
  )
  override fun listWorkspacesPaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): WorkspaceReadList =
    execute {
      workspacesHandler.listWorkspacesPaginated(
        listResourcesForWorkspacesRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.WORKSPACE)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceWriteRule::class,
  )
  override fun updateWorkspace(
    @Body workspaceUpdate: WorkspaceUpdate,
  ): WorkspaceRead = execute { workspacesHandler.updateWorkspace(workspaceUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceWriteRule::class,
  )
  override fun updateWorkspaceFeedback(
    @Body workspaceGiveFeedback: WorkspaceGiveFeedback,
  ) {
    execute<Any?> {
      workspacesHandler.setFeedbackDone(workspaceGiveFeedback)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.WORKSPACE)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceWriteRule::class,
  )
  override fun updateWorkspaceName(
    @Body workspaceUpdateName: WorkspaceUpdateName,
  ): WorkspaceRead = execute { workspacesHandler.updateWorkspaceName(workspaceUpdateName) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.WORKSPACE)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceTransferRule::class,
  )
  override fun updateWorkspaceOrganization(
    @Body workspaceUpdateOrganization: WorkspaceUpdateOrganization,
  ): WorkspaceRead = execute { workspacesHandler.updateWorkspaceOrganization(workspaceUpdateOrganization) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceReadRule::class,
  )
  override fun getWorkspaceByConnectionId(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): WorkspaceRead = execute { workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, false) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceTombstoneConnectionRule::class,
  )
  override fun getWorkspaceByConnectionIdWithTombstone(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): WorkspaceRead = execute { workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody, true) }

  // No organization role is required because this endpoint can list workspaces within an org that a
  // user does not have an org-level permission for. This endpoint only returns workspaces that the
  // user has a permission for, so it is safe to authorize any authenticated user.
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceFilteredOrganizationRule::class,
  )
  override fun listWorkspacesInOrganization(
    @Body listWorkspacesInOrganizationRequestBody: ListWorkspacesInOrganizationRequestBody?,
  ): WorkspaceReadList =
    execute {
      workspacesHandler.listWorkspacesInOrganizationForUser(
        userId = currentUserService.getCurrentUser().userId,
        request = listWorkspacesInOrganizationRequestBody.requireBody(),
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceUserListRule::class,
  )
  override fun listWorkspacesByUser(
    @Body request: ListWorkspacesByUserRequestBody?,
  ): WorkspaceReadList = execute { workspacesHandler.listWorkspacesByUser(request.requireBody()) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WorkspaceWriteUnsupportedRule::class,
  )
  override fun getAvailableDbtJobsForWorkspace(workspaceGetDbtJobsRequest: WorkspaceGetDbtJobsRequest): WorkspaceGetDbtJobsResponse =
    throw ApiNotImplementedInOssProblem()
}
