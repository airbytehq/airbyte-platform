/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.FusionSourceEnablementApi
import io.airbyte.api.model.generated.FusionSourceEnablementCreate
import io.airbyte.api.model.generated.FusionSourceEnablementRead
import io.airbyte.api.model.generated.FusionSourceEnablementUpdate
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

@Controller("/api/v1/sources/{sourceId}/enablement")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionSourceEnablementApiController : FusionSourceEnablementApi {
  @Get
  override fun getFusionSourceEnablement(
    @PathVariable sourceId: UUID,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @Post
  override fun createFusionSourceEnablement(
    @PathVariable sourceId: UUID,
    @Body request: FusionSourceEnablementCreate,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @Put
  override fun updateFusionSourceEnablement(
    @PathVariable sourceId: UUID,
    @Body request: FusionSourceEnablementUpdate,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @Delete
  override fun deleteFusionSourceEnablement(
    @PathVariable sourceId: UUID,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()
}
