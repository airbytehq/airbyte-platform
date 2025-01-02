/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.JobRetryStatesApi;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.model.generated.RetryStateRead;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.server.handlers.RetryStatesHandler;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;

@Controller("/api/v1/jobs/retry_states")
public class JobRetryStatesApiController implements JobRetryStatesApi {

  private final RetryStatesHandler handler;

  public JobRetryStatesApiController(final RetryStatesHandler handler) {
    this.handler = handler;
  }

  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/get")
  @Override
  public RetryStateRead get(@Body final JobIdRequestBody req) {
    final var found = handler.getByJobId(req);

    if (found.isEmpty()) {
      throw new IdNotFoundKnownException(String.format("Could not find Retry State for job_id: %d.", req.getId()), String.valueOf(req.getId()));
    }

    return found.get();
  }

  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/create_or_update")
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void createOrUpdate(@Body final JobRetryStateRequestBody req) {
    handler.putByJobId(req);
  }

}
