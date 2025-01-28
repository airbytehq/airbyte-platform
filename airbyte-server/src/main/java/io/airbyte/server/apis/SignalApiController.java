/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.SignalApi;
import io.airbyte.api.model.generated.SignalInput;
import io.airbyte.commons.server.handlers.SignalHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/signal")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class SignalApiController implements SignalApi {

  final SignalHandler signalHandler;

  public SignalApiController(SignalHandler signalHandler) {
    this.signalHandler = signalHandler;
  }

  @Override
  @Post
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({ADMIN})
  public void signal(@Body SignalInput signalInput) {
    ApiHelper.execute(() -> {
      final io.airbyte.config.SignalInput internalSignalInput = new io.airbyte.config.SignalInput(
          signalInput.getWorkflowType(),
          signalInput.getWorkflowId());
      signalHandler.signal(internalSignalInput);
      return null;
    });
  }

}
