/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import com.fasterxml.jackson.core.JsonParseException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Exception for invalid json input.
 */
@Provider
public class InvalidJsonExceptionMapper implements ExceptionMapper<JsonParseException> {

  @Override
  public Response toResponse(final JsonParseException e) {
    return Response.status(422)
        .entity(KnownException.infoFromThrowableWithMessage(e, "Invalid json. " + e.getMessage() + " " + e.getOriginalMessage()))
        .type("application/json")
        .build();
  }

}
