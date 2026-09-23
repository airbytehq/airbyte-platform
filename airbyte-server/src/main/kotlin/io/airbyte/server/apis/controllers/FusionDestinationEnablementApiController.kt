/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.FusionDestinationEnablementApi
import io.airbyte.api.model.generated.FusionDestinationEnablementCreate
import io.airbyte.api.model.generated.FusionDestinationEnablementRead
import io.airbyte.api.model.generated.FusionDestinationEnablementUpdate
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

@Controller("/api/v1/destinations/{destinationId}/enablement")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionDestinationEnablementApiController : FusionDestinationEnablementApi {
  @Get
  override fun getFusionDestinationEnablement(
    @PathVariable destinationId: UUID,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @Post
  override fun createFusionDestinationEnablement(
    @PathVariable destinationId: UUID,
    @Body request: FusionDestinationEnablementCreate,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @Put
  override fun updateFusionDestinationEnablement(
    @PathVariable destinationId: UUID,
    @Body request: FusionDestinationEnablementUpdate,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @Delete
  override fun deleteFusionDestinationEnablement(
    @PathVariable destinationId: UUID,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()
}
