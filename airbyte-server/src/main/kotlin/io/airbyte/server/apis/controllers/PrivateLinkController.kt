/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.PrivateLinkApi
import io.airbyte.api.model.generated.PrivateLinkCreateRequestBody
import io.airbyte.api.model.generated.PrivateLinkIdRequestBody
import io.airbyte.api.model.generated.PrivateLinkListRequestBody
import io.airbyte.api.model.generated.PrivateLinkRead
import io.airbyte.api.model.generated.PrivateLinkReadList
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/private_link")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class PrivateLinkController : PrivateLinkApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @Post("/create")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createPrivateLink(
    @Body privateLinkCreateRequestBody: PrivateLinkCreateRequestBody,
  ): PrivateLinkRead = throw ApiNotImplementedInOssProblem()

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @Post("/list")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPrivateLinksForWorkspace(
    @Body privateLinkListRequestBody: PrivateLinkListRequestBody,
  ): PrivateLinkReadList = throw ApiNotImplementedInOssProblem()

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @Post("/delete")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deletePrivateLink(
    @Body privateLinkIdRequestBody: PrivateLinkIdRequestBody,
  ): Unit = throw ApiNotImplementedInOssProblem()
}
