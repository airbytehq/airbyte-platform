/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.airbyte.commons.json.Jsons;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Exception mapper for when an input is invalid json.
 */
@Provider
public class InvalidJsonInputExceptionMapper implements ExceptionMapper<JsonMappingException> {

  @Override
  public Response toResponse(final JsonMappingException e) {
    return Response.status(422)
        .entity(
            Jsons.serialize(KnownException.infoFromThrowableWithMessage(e, "Invalid json input. " + e.getMessage() + " " + e.getOriginalMessage())))
        .type("application/json")
        .build();
  }

}
