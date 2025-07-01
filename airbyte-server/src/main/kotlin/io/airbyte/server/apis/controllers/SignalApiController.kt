/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SignalApi
import io.airbyte.api.model.generated.SignalInput
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.SignalHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/signal")
@Secured(SecurityRule.IS_AUTHENTICATED)
class SignalApiController(
  val signalHandler: SignalHandler,
) : SignalApi {
  @Post
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  override fun signal(
    @Body signalInput: SignalInput,
  ) {
    execute<Any?> {
      val internalSignalInput =
        io.airbyte.config.SignalInput(
          signalInput.workflowType,
          signalInput.workflowId,
        )
      signalHandler.signal(internalSignalInput)
      null
    }
  }
}
