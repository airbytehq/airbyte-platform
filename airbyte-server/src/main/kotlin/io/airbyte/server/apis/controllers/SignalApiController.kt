/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SignalApi
import io.airbyte.api.server.generated.models.SignalInput
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.SignalHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class SignalApiController(
  val signalHandler: SignalHandler,
) : SignalApi {
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
