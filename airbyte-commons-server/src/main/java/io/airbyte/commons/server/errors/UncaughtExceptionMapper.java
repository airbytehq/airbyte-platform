/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.airbyte.api.model.generated.KnownExceptionInfo;
import io.airbyte.commons.json.Jsons;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mapper for when an unknown exception is thrown.
 */
@Provider
public class UncaughtExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UncaughtExceptionMapper.class);

  @Override
  public Response toResponse(final Throwable e) {
    LOGGER.error("Uncaught exception", e);
    final KnownExceptionInfo exceptionInfo = KnownException.infoFromThrowableWithMessage(e, "Internal Server Error: " + e.getMessage());
    return Response.status(500)
        .entity(Jsons.serialize(exceptionInfo))
        .type("application/json")
        .build();
  }

}
