/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import datadog.trace.api.Trace
import io.airbyte.api.model.generated.ListOrganizationSummariesRequestBody
import io.airbyte.api.model.generated.ListOrganizationSummariesResponse
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationInfoRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.api.model.generated.OrganizationSummary
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.tools.StringUtils
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

@Singleton
open class OrganizationsHandler(
  private val organizationPersistence: OrganizationPersistence,
  private val permissionHandler: PermissionHandler,
  @Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
  private val organizationPaymentConfigService: OrganizationPaymentConfigService,
  private val workspacesHandler: WorkspacesHandler,
  private val permissionService: PermissionService,
) {
  companion object {
    private fun buildOrganizationRead(organization: Organization): OrganizationRead =
      OrganizationRead()
        .organizationId(organization.organizationId)
        .organizationName(organization.name)
        .email(organization.email)
        .ssoRealm(organization.ssoRealm)
  }

  @Throws(IOException::class, JsonValidationException::class, PermissionRedundantException::class)
  fun createOrganization(request: OrganizationCreateRequestBody): OrganizationRead {
    val organizationName = request.organizationName
    val email = request.email
    val userId = request.userId
    val orgId = uuidGenerator.get()
    val organization =
      Organization()
        .withOrganizationId(orgId)
        .withName(organizationName)
        .withEmail(email)
        .withUserId(userId)
    organizationPersistence.createOrganization(organization)

    // Also create an OrgAdmin permission.
    permissionHandler.createPermission(
      Permission()
        .withPermissionId(uuidGenerator.get())
        .withUserId(userId)
        .withOrganizationId(orgId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
    )

    organizationPaymentConfigService.saveDefaultPaymentConfig(organization.organizationId)
    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun updateOrganization(request: OrganizationUpdateRequestBody): OrganizationRead {
    val organizationId = request.organizationId
    val organization =
      organizationPersistence
        .getOrganization(organizationId)
        .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.ORGANIZATION, organizationId) }

    var hasChanged = false
    if (organization.name != request.organizationName) {
      organization.name = request.organizationName
      hasChanged = true
    }

    if (request.email != null && request.email != organization.email) {
      organization.email = request.email
      hasChanged = true
    }

    if (hasChanged) {
      organizationPersistence.updateOrganization(organization)
    }

    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getOrganization(request: OrganizationIdRequestBody): OrganizationRead {
    val organizationId = request.organizationId
    val organization =
      organizationPersistence
        .getOrganization(organizationId)
        .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.ORGANIZATION, organizationId) }

    return buildOrganizationRead(organization)
  }

  @Throws(IOException::class)
  fun listOrganizationsByUser(request: ListOrganizationsByUserRequestBody): OrganizationReadList {
    val nameContains =
      if (StringUtils.isBlank(request.nameContains)) {
        Optional.empty<String>()
      } else {
        Optional.of(request.nameContains)
      }

    val organizationReadList =
      if (request.pagination != null) {
        organizationPersistence
          .listOrganizationsByUserIdPaginated(
            ResourcesByUserQueryPaginated(
              request.userId,
              false,
              request.pagination.pageSize,
              request.pagination.rowOffset,
            ),
            nameContains,
          ).map { buildOrganizationRead(it) }
      } else {
        organizationPersistence
          .listOrganizationsByUserId(request.userId, nameContains)
          .map { buildOrganizationRead(it) }
      }

    return OrganizationReadList().organizations(organizationReadList)
  }

  /**
   * This function makes several calls and aggregates results from multiple areas to provide a more comprehensive
   * summary of an organizations state. This is meant to serve the org/workspace picker for the org landing page.
   * The logic is relatively confusing: we accept a filter value that applied to both the organization name AND
   * the workspace name, which can result in what appears to be missing data. I've tried to summarize some of this here:
   *
   *   1. When nameContains filters out all orgs but results in workspaces: in this case, we go through the workspace
   *      id set and select the orgs for each workspace so we can associate them.
   *   2. When nameContains filters out all the workspaces but results in orgs: here, we loop through all the org results
   *      that do not contain workspaces in their lists and select those workspaces. ***This selection respects the original
   *      filters and page size!***
   *   3. We maintain a separate map of member counts, which is associated to the org id. We join this with the original org
   *      when we create the response body.
   * As a result, we potentially make all the following db queries:
   *   1. get orgs by user id
   *   2. get workspaces by user id
   *   3. get member counts by org id
   *   4. get workspaces by org id
   */
  @Trace
  fun getOrganizationSummaries(request: ListOrganizationSummariesRequestBody): ListOrganizationSummariesResponse {
    val orgListReq =
      ListOrganizationsByUserRequestBody()
        .userId(request.userId)
        .nameContains(request.nameContains)
        .pagination(request.pagination)

    val orgListResp: OrganizationReadList = this.listOrganizationsByUser(orgListReq)

    val workspaceListReq =
      ListWorkspacesByUserRequestBody()
        .userId(request.userId)
        .nameContains(request.nameContains)
        .pagination(request.pagination)

    val workspaceListResp: WorkspaceReadList = workspacesHandler.listWorkspacesByUser(workspaceListReq)

    // If we have workspaces in the filtered list that are NOT in the previously selected
    // org list, we need to select those as well. This can happen when the nameFilter filters out
    // all orgs, but includes a workspace. I suspect that this list will be quite small.
    val orgSet = orgListResp.organizations.map { it.organizationId }.toSet()
    val orgIdsToRetrieve =
      workspaceListResp.workspaces
        .filter { !orgSet.contains(it.organizationId) }
        .map { it.organizationId }
        .toSet()

    for (orgId in orgIdsToRetrieve) {
      val retrieved = this.getOrganization(OrganizationIdRequestBody().organizationId(orgId))
      orgListResp.addOrganizationsItem(retrieved)
    }

    val memberCounts =
      permissionService.getMemberCountsForOrganizationList(
        orgListResp.organizations.map { it.organizationId },
      )
    val orgIdToMemberCount = memberCounts.associate { it.organizationId to it.count }

    val orgIdToWorkspaceMap = mutableMapOf<UUID, MutableList<WorkspaceRead>>()
    for (workspace in workspaceListResp.workspaces) {
      orgIdToWorkspaceMap.getOrPut(workspace.organizationId) { mutableListOf() }.add(workspace)
    }

    // It's possible, due to filtering rules, that we end up with orgs in the orgs list that do not
    // contain workspaces. We want to show at least some workspaces here (still respecting the original filtering/pagination rules)
    // so we attempt to get them here
    val fullyPopulatedOrgMap: Map<UUID, List<WorkspaceRead>> =
      orgIdToWorkspaceMap.mapValues {
        if (it.value.isEmpty()) {
          val req =
            ListWorkspacesInOrganizationRequestBody()
              .organizationId(it.key)
              .nameContains(request.nameContains)
              .pagination(
                Pagination()
                  .pageSize(request.pagination.pageSize)
                  .rowOffset(request.pagination.rowOffset),
              )
          val workspaces = workspacesHandler.listWorkspacesInOrganization(req)
          workspaces.workspaces
        } else {
          it.value
        }
      }

    val orgSummaries = mutableListOf<OrganizationSummary>()
    for (org in orgListResp.organizations) {
      val summary =
        OrganizationSummary()
          .organization(org)
          .workspaces(fullyPopulatedOrgMap[org.organizationId])
          .memberCount(orgIdToMemberCount[org.organizationId])
      orgSummaries.add(summary)
    }

    return ListOrganizationSummariesResponse()
      .organizationSummaries(orgSummaries)
  }

  private fun buildOrganizationInfoRead(organization: Organization): OrganizationInfoRead =
    OrganizationInfoRead()
      .organizationId(organization.organizationId)
      .organizationName(organization.name)
      .sso(organization.ssoRealm != null && organization.ssoRealm.isNotEmpty())

  fun getOrganizationInfo(organizationId: UUID): OrganizationInfoRead {
    val organization = organizationPersistence.getOrganization(organizationId)
    if (organization.isEmpty) {
      throw io.airbyte.data.ConfigNotFoundException(ConfigNotFoundType.ORGANIZATION, organizationId.toString())
    }
    return buildOrganizationInfoRead(organization.get())
  }
}
