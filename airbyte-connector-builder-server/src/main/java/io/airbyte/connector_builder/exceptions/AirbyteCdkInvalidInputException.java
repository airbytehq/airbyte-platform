/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.airbyte.commons.server.errors.KnownException;
import io.airbyte.protocol.models.AirbyteTraceMessage;

/**
 * Thrown when the CDK processed the request, but the result contains an error.
 */
@SuppressWarnings("PMD.NonSerializableClass")
public class AirbyteCdkInvalidInputException extends KnownException {

  AirbyteTraceMessage trace;

  public AirbyteCdkInvalidInputException(final String message, final AirbyteTraceMessage trace) {
    super(message);
    this.trace = trace;
  }

  public AirbyteCdkInvalidInputException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return 422;
  }

}
