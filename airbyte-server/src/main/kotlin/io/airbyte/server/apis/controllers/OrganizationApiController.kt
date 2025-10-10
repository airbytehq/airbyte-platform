/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OrganizationApi
import io.airbyte.api.model.generated.DataWorkerUsage
import io.airbyte.api.model.generated.ListOrganizationSummariesRequestBody
import io.airbyte.api.model.generated.ListOrganizationSummariesResponse
import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationDataWorkerUsageRead
import io.airbyte.api.model.generated.OrganizationDataWorkerUsageRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationInfoRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.model.generated.OrganizationUsageRead
import io.airbyte.api.model.generated.OrganizationUsageRequestBody
import io.airbyte.api.model.generated.RegionDataWorkerUsage
import io.airbyte.api.model.generated.WorkspaceDataWorkerUsage
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.time.LocalDate
import java.util.UUID
import kotlin.random.Random

@Controller("/api/v1/organizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationApiController(
  val organizationsHandler: OrganizationsHandler,
  val organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper,
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
      val organizationId = organizationIdRequestBody.organizationId

      organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId)

      organizationsHandler.getOrganizationInfo(organizationId)
    }

  @Post("/get_data_worker_usage")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationDataWorkerUsage(
    @Body organizationDataWorkerUsageRequestBody: OrganizationDataWorkerUsageRequestBody,
  ): OrganizationDataWorkerUsageRead? =
    execute {
      val regionNames = listOf("US-WEST-1", "US-WEST-2", "US-EAST-1")
      val workspacesPerRegion = 3

      fun dataWorkers(
        start: LocalDate,
        end: LocalDate,
      ) = buildList {
        var d = start
        while (!d.isAfter(end)) {
          add(DataWorkerUsage().date(d).used(Random.nextDouble(0.0, 3.0)))
          d = d.plusDays(1)
        }
      }

      OrganizationDataWorkerUsageRead()
        .organizationId(organizationDataWorkerUsageRequestBody.organizationId)
        .regions(
          regionNames.map { regionName ->
            RegionDataWorkerUsage()
              .id(UUID.randomUUID())
              .name(regionName)
              .workspaces(
                (1..workspacesPerRegion).map { idx ->
                  WorkspaceDataWorkerUsage()
                    .id(UUID.randomUUID())
                    .name("$regionName workspace $idx")
                    .dataWorkers(dataWorkers(organizationDataWorkerUsageRequestBody.startDate, organizationDataWorkerUsageRequestBody.endDate))
                },
              )
          },
        )
    }
}
