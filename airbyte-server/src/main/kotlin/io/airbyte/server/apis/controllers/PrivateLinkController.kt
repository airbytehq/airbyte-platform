/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.PrivateLinkApi
import io.airbyte.api.server.generated.models.PrivateLinkCreateRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkIdRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkInternalIdRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkListByStatusRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkListRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkRead
import io.airbyte.api.server.generated.models.PrivateLinkReadList
import io.airbyte.api.server.generated.models.PrivateLinkUpdateRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.PrivateLinkGlobalStubRule
import io.airbyte.server.auth.PrivateLinkWorkspaceStubRule
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class PrivateLinkController : PrivateLinkApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkWorkspaceStubRule::class,
  )
  override fun createPrivateLink(
    @Body privateLinkCreateRequestBody: PrivateLinkCreateRequestBody?,
  ): PrivateLinkRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkWorkspaceStubRule::class,
  )
  override fun listPrivateLinksForWorkspace(
    @Body privateLinkListRequestBody: PrivateLinkListRequestBody?,
  ): PrivateLinkReadList = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkWorkspaceStubRule::class,
  )
  override fun deletePrivateLink(
    @Body privateLinkIdRequestBody: PrivateLinkIdRequestBody,
  ): Unit = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkGlobalStubRule::class,
  )
  override fun getPrivateLink(
    @Body privateLinkInternalIdRequestBody: PrivateLinkInternalIdRequestBody,
  ): PrivateLinkRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkGlobalStubRule::class,
  )
  override fun updatePrivateLink(
    @Body privateLinkUpdateRequestBody: PrivateLinkUpdateRequestBody,
  ): PrivateLinkRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = PrivateLinkGlobalStubRule::class,
  )
  override fun listPrivateLinksByStatus(
    @Body privateLinkListByStatusRequestBody: PrivateLinkListByStatusRequestBody,
  ): PrivateLinkReadList = throw ApiNotImplementedInOssProblem()
}
