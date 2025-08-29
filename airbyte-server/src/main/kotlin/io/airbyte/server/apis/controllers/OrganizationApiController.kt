/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OrganizationApi
import io.airbyte.api.model.generated.ListOrganizationSummariesRequestBody
import io.airbyte.api.model.generated.ListOrganizationSummariesResponse
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationInfoRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.model.generated.OrganizationUsageRead
import io.airbyte.api.model.generated.OrganizationUsageRequestBody
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.Optional

@Controller("/api/v1/organizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationApiController(
  val organizationsHandler: OrganizationsHandler,
  val roleResolver: RoleResolver,
  val workspacePersistence: WorkspacePersistence,
) : OrganizationApi {
  @Post("/get")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationRead? = execute { organizationsHandler.getOrganization(organizationIdRequestBody) }

  @Post("/update")
  @Secured(AuthRoleConstants.ORGANIZATION_EDITOR)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateOrganization(
    @Body organizationUpdateRequestBody: OrganizationUpdateRequestBody,
  ): OrganizationRead? = execute { organizationsHandler.updateOrganization(organizationUpdateRequestBody) }

  @Post("/create")
  @Secured(AuthRoleConstants.ADMIN) // instance admin only
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createOrganization(
    @Body organizationCreateRequestBody: OrganizationCreateRequestBody,
  ): OrganizationRead? = execute { organizationsHandler.createOrganization(organizationCreateRequestBody) }

  @Post("/delete")
  @Secured(AuthRoleConstants.ADMIN) // instance admin only
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ) {
    // To be implemented; we need a tombstone column for organizations table.
  }

  @Post("/list_by_user_id")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.SELF)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listOrganizationsByUser(
    @Body request: ListOrganizationsByUserRequestBody,
  ): OrganizationReadList? = execute { organizationsHandler.listOrganizationsByUser(request) }

  @Post("/get_usage")
  @RequiresIntent(Intent.ViewOrganizationUsage)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationUsage(
    @Body organizationUsageRequestBody: OrganizationUsageRequestBody?,
  ): OrganizationUsageRead = throw ApiNotImplementedInOssProblem()

  @Post("/list_summaries")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.SELF)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listOrganizationSummaries(
    @Body organizationSummaryRequestBody: ListOrganizationSummariesRequestBody,
  ): ListOrganizationSummariesResponse? = execute { organizationsHandler.getOrganizationSummaries(organizationSummaryRequestBody) }

  @Post("/get_organization_info")
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrgInfo(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationInfoRead? =
    execute {
      // Check if user has organization-level access OR workspace-level access to any workspace in this org
      val organizationId = organizationIdRequestBody.organizationId

      // First try organization-level permissions
      val orgAuth =
        roleResolver
          .newRequest()
          .withCurrentAuthentication()
          .withOrg(organizationId)

      try {
        orgAuth.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER)
        // User has org-level access, proceed
      } catch (e: ForbiddenProblem) {
        // No org-level access, check workspace-level access
        val workspacesInOrg =
          workspacePersistence.listWorkspacesByOrganizationId(
            organizationId = organizationId,
            includeDeleted = false,
            keyword = Optional.empty(),
          )
        val workspaceIds = workspacesInOrg.map { it.workspaceId }

        if (workspaceIds.isNotEmpty()) {
          val workspaceAuth =
            roleResolver
              .newRequest()
              .withCurrentAuthentication()
              .withWorkspaces(workspaceIds)

          // This will throw if user doesn't have workspace access to any workspace in the org
          workspaceAuth.requireRole(AuthRoleConstants.WORKSPACE_READER)
        } else {
          // No workspaces in org, re-throw the original org permission error
          throw e
        }
      }

      organizationsHandler.getOrganizationInfo(organizationIdRequestBody.organizationId)
    }
}
