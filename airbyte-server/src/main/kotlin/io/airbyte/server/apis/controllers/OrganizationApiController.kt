/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.OrganizationApi
import io.airbyte.api.server.generated.models.ListOrganizationSummariesRequestBody
import io.airbyte.api.server.generated.models.ListOrganizationSummariesResponse
import io.airbyte.api.server.generated.models.ListOrganizationsByUserRequestBody
import io.airbyte.api.server.generated.models.OrganizationAgenticStatusUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationCreateRequestBody
import io.airbyte.api.server.generated.models.OrganizationDataWorkerUsageRead
import io.airbyte.api.server.generated.models.OrganizationDataWorkerUsageRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationInfoRead
import io.airbyte.api.server.generated.models.OrganizationRead
import io.airbyte.api.server.generated.models.OrganizationReadList
import io.airbyte.api.server.generated.models.OrganizationUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationUsageRead
import io.airbyte.api.server.generated.models.OrganizationUsageRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.OrganizationAccessibleRule
import io.airbyte.server.auth.OrganizationCreateRule
import io.airbyte.server.auth.OrganizationDeletePlaceholderRule
import io.airbyte.server.auth.OrganizationEditorRule
import io.airbyte.server.auth.OrganizationInstanceAdminRule
import io.airbyte.server.auth.OrganizationMemberUnsupportedRule
import io.airbyte.server.auth.OrganizationUsageUnsupportedRule
import io.airbyte.server.auth.OrganizationUserListRule
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.validation.Validated
import io.micronaut.validation.annotation.ValidatedElement
import jakarta.validation.constraints.NotNull

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationApiController(
  val organizationsHandler: OrganizationsHandler,
  val organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper,
) : OrganizationApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationAccessibleRule::class,
  )
  override fun getOrganization(
    @Body organizationIdRequestBody: OrganizationIdRequestBody?,
  ): OrganizationRead =
    execute {
      val request = organizationIdRequestBody.requireBody()

      organizationsHandler.getOrganization(request)
    }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationEditorRule::class,
  )
  override fun updateOrganization(
    @Body organizationUpdateRequestBody: OrganizationUpdateRequestBody?,
  ): OrganizationRead = execute { organizationsHandler.updateOrganization(organizationUpdateRequestBody.requireBody()) }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationInstanceAdminRule::class,
  )
  override fun setOrganizationAgenticStatus(
    @Body organizationAgenticStatusUpdateRequestBody: OrganizationAgenticStatusUpdateRequestBody?,
  ): OrganizationRead = execute { organizationsHandler.setOrganizationAgenticStatus(organizationAgenticStatusUpdateRequestBody.requireBody()) }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationCreateRule::class,
  )
  override fun createOrganization(
    @Body organizationCreateRequestBody: OrganizationCreateRequestBody?,
  ): OrganizationRead = execute { organizationsHandler.createOrganization(organizationCreateRequestBody.requireBody()) }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @Validated
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationDeletePlaceholderRule::class,
  )
  override fun deleteOrganization(
    // The validation processor is not configured, so the parameter needs an explicit validation marker.
    @Body @NotNull @ValidatedElement organizationIdRequestBody: OrganizationIdRequestBody,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationUserListRule::class,
  )
  override fun listOrganizationsByUser(
    @Body request: ListOrganizationsByUserRequestBody?,
  ): OrganizationReadList = execute { organizationsHandler.listOrganizationsByUser(request.requireBody()) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationUsageUnsupportedRule::class,
  )
  override fun getOrganizationUsage(
    @Body organizationUsageRequestBody: OrganizationUsageRequestBody?,
  ): OrganizationUsageRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationUserListRule::class,
  )
  override fun listOrganizationSummaries(
    @Body organizationSummaryRequestBody: ListOrganizationSummariesRequestBody?,
  ): ListOrganizationSummariesResponse = execute { organizationsHandler.getOrganizationSummaries(organizationSummaryRequestBody.requireBody()) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationAccessibleRule::class,
  )
  override fun getOrgInfo(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationInfoRead =
    execute {
      val organizationId = organizationIdRequestBody.organizationId

      organizationsHandler.getOrganizationInfo(organizationId)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationMemberUnsupportedRule::class,
  )
  override fun getOrganizationDataWorkerUsage(
    @Body organizationDataWorkerUsageRequestBody: OrganizationDataWorkerUsageRequestBody?,
  ): OrganizationDataWorkerUsageRead = throw ApiNotImplementedInOssProblem()
}
