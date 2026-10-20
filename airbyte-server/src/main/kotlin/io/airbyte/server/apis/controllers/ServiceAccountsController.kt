/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ServiceAccountsApi
import io.airbyte.api.server.generated.models.AccessToken
import io.airbyte.api.server.generated.models.ServiceAccountTokenRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.data.services.ServiceAccountsService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
class ServiceAccountsController(
  val serviceAccountsService: ServiceAccountsService,
) : ServiceAccountsApi {
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.LOGIN,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun getServiceAccountToken(request: ServiceAccountTokenRequestBody): AccessToken =
    AccessToken(serviceAccountsService.generateToken(request.serviceAccountId, request.secret))
}
