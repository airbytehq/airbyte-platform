/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.StateApi
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionState
import io.airbyte.api.server.generated.models.ConnectionStateCreateOrUpdate
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class StateApiController(
  private val stateHandler: StateHandler,
) : StateApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ConnectionStateWorkerRule::class,
  )
  override fun createOrUpdateState(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState = execute { stateHandler.createOrUpdateState(connectionStateCreateOrUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ConnectionStateWriteRule::class,
  )
  override fun createOrUpdateStateSafe(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState = execute { stateHandler.createOrUpdateStateSafe(connectionStateCreateOrUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ConnectionStateReadRule::class,
  )
  override fun getState(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionState = execute { stateHandler.getState(connectionIdRequestBody) }
}
