/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OrganizationSemanticSearchApi
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationSemanticSearchEnablementRead
import io.airbyte.api.model.generated.OrganizationSemanticSearchEnablementUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/organizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OrganizationSemanticSearchApiController : OrganizationSemanticSearchApi {
  @Post("/get_semantic_search_enablement")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOrganizationSemanticSearchEnablement(
    @Body organizationIdRequestBody: OrganizationIdRequestBody,
  ): OrganizationSemanticSearchEnablementRead = throw ApiNotImplementedInOssProblem()

  @Post("/update_semantic_search_enablement")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateOrganizationSemanticSearchEnablement(
    @Body organizationSemanticSearchEnablementUpdateRequestBody: OrganizationSemanticSearchEnablementUpdateRequestBody,
  ): OrganizationSemanticSearchEnablementRead = throw ApiNotImplementedInOssProblem()
}
