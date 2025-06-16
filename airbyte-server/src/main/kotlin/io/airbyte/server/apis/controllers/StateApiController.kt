/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.StateApi
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/state")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class StateApiController(
  private val stateHandler: StateHandler,
) : StateApi {
  @Post("/create_or_update")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createOrUpdateState(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState? = execute { stateHandler.createOrUpdateState(connectionStateCreateOrUpdate) }

  @Post("/create_or_update_safe")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createOrUpdateStateSafe(
    @Body connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate,
  ): ConnectionState? = execute { stateHandler.createOrUpdateStateSafe(connectionStateCreateOrUpdate) }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getState(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionState? = execute { stateHandler.getState(connectionIdRequestBody) }
}
