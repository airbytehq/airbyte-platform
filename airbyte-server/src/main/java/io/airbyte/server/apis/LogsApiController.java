/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.LogsApi;
import io.airbyte.api.model.generated.LogsRequestBody;
import io.airbyte.commons.server.handlers.LogsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.io.File;

@SuppressWarnings("MissingJavadocType")
@Controller("/api/v1/logs")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class LogsApiController implements LogsApi {

  private final LogsHandler logsHandler;

  public LogsApiController(final LogsHandler logsHandler) {
    this.logsHandler = logsHandler;
  }

  @Post("/get")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public File getLogs(final LogsRequestBody logsRequestBody) {
    return ApiHelper.execute(() -> logsHandler.getLogs(logsRequestBody));
  }

}
