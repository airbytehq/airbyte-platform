/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.StreamStatusesApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.server.handlers.StreamStatusesHandler;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;

@Controller("/api/v1/stream_statuses")
public class StreamStatusesApiController implements StreamStatusesApi {

  private final StreamStatusesHandler handler;

  public StreamStatusesApiController(final StreamStatusesHandler handler) {
    this.handler = handler;
  }

  @Status(HttpStatus.CREATED)
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/create")
  @Override
  public StreamStatusRead createStreamStatus(final StreamStatusCreateRequestBody req) {
    Validations.validate(req.getRunState(), req.getIncompleteRunCause());

    return handler.createStreamStatus(req);
  }

  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/update")
  @Override
  public StreamStatusRead updateStreamStatus(final StreamStatusUpdateRequestBody req) {
    Validations.validate(req.getRunState(), req.getIncompleteRunCause());

    return handler.updateStreamStatus(req);
  }

  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/list")
  @Override
  public StreamStatusReadList getStreamStatuses(final StreamStatusListRequestBody req) {
    Validations.validate(req.getPagination());

    return handler.listStreamStatus(req);
  }

  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/latest_per_run_state")
  @Override
  public StreamStatusReadList getStreamStatusesByRunState(final ConnectionIdRequestBody req) {
    return handler.listStreamStatusPerRunState(req);
  }

  /**
   * Stateless request body validations.
   */
  static class Validations {

    static int PAGE_MIN = 1;

    static int OFFSET_MIN = 0;

    static void validate(final StreamStatusRunState runState, final StreamStatusIncompleteRunCause incompleteRunCause) {
      if (runState != StreamStatusRunState.INCOMPLETE && incompleteRunCause != null) {
        throw new BadRequestException("Incomplete run cause may only be set for runs that stopped in an incomplete state.") {};
      }
      if (runState == StreamStatusRunState.INCOMPLETE && incompleteRunCause == null) {
        throw new BadRequestException("Incomplete run cause must be set for runs that stopped in an incomplete state.") {};
      }
    }

    static void validate(final Pagination pagination) {
      if (pagination == null) {
        throw new BadRequestException("Pagination params must be provided.");
      }
      if (pagination.getPageSize() < PAGE_MIN) {
        throw new BadRequestException("Page size must be at least 1.");
      }
      if (pagination.getRowOffset() < OFFSET_MIN) {
        throw new BadRequestException("Row offset cannot be less than 0.");
      }
      if (pagination.getRowOffset() % pagination.getPageSize() > 0) {
        throw new BadRequestException("Row offset must be evenly divisible by page size.");
      }
    }

  }

}
