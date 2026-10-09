/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.FusionConnectionEnablementApi
import io.airbyte.api.model.generated.FusionConnectionEnablementCreate
import io.airbyte.api.model.generated.FusionConnectionEnablementRead
import io.airbyte.api.model.generated.FusionConnectionEnablementUpdate
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Delete
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Put
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

@Controller("/api/v1/connections/{connectionId}/enablement")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionConnectionEnablementApiController : FusionConnectionEnablementApi {
  @Get
  override fun getFusionConnectionEnablement(
    @PathVariable connectionId: UUID,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @Post
  override fun createFusionConnectionEnablement(
    @PathVariable connectionId: UUID,
    @Body request: FusionConnectionEnablementCreate,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @Put
  override fun updateFusionConnectionEnablement(
    @PathVariable connectionId: UUID,
    @Body request: FusionConnectionEnablementUpdate,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @Delete
  override fun deleteFusionConnectionEnablement(
    @PathVariable connectionId: UUID,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()
}
