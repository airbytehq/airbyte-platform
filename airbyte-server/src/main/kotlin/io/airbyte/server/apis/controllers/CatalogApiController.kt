/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.CatalogApi
import io.airbyte.api.server.generated.models.DiffCatalogsRequest
import io.airbyte.api.server.generated.models.DiffCatalogsResponse
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.metrics.lib.TracingHelper
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.CatalogDiffRule
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
class CatalogApiController(
  private val catalogDiffService: CatalogDiffService,
) : CatalogApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = CatalogDiffRule::class,
  )
  override fun diffCatalogs(
    @Body diffCatalogsRequestBody: DiffCatalogsRequest?,
  ): DiffCatalogsResponse {
    val diffCatalogsRequest = diffCatalogsRequestBody.requireBody()
    // Validate that connection_id is provided when persist_changes is true
    if (diffCatalogsRequest.persistChanges == true && diffCatalogsRequest.connectionId == null) {
      throw BadRequestProblem(
        ProblemMessageData().message("connection_id is required when persist_changes is true"),
      )
    }

    diffCatalogsRequest.connectionId?.let { TracingHelper.addConnection(it) }
    return catalogDiffService.diffCatalogs(diffCatalogsRequest)
  }
}
