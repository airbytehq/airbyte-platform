/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.StateApi
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionState
import io.airbyte.api.server.generated.models.ConnectionStateCreateOrUpdate
import io.airbyte.commons.auth.roles.AuthRoleConstants
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
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createOrUpdateState(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState = execute { stateHandler.createOrUpdateState(connectionStateCreateOrUpdate) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createOrUpdateStateSafe(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState = execute { stateHandler.createOrUpdateStateSafe(connectionStateCreateOrUpdate) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getState(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionState = execute { stateHandler.getState(connectionIdRequestBody) }
}
