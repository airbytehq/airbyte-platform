/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.OrganizationSemanticSearchApi
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationSemanticSearchEnablementRead
import io.airbyte.api.server.generated.models.OrganizationSemanticSearchEnablementUpdateRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.OssOrganizationAdminUnsupportedRule
import io.airbyte.server.auth.OssUnsupportedOperationRule
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationSemanticSearchApiController : OrganizationSemanticSearchApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OssUnsupportedOperationRule::class,
  )
  override fun getOrganizationSemanticSearchEnablement(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationSemanticSearchEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OssOrganizationAdminUnsupportedRule::class,
  )
  override fun updateOrganizationSemanticSearchEnablement(
    @Body organizationSemanticSearchEnablementUpdateRequestBody: OrganizationSemanticSearchEnablementUpdateRequestBody,
  ): OrganizationSemanticSearchEnablementRead = throw ApiNotImplementedInOssProblem()
}
