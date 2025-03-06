/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Thrown when the server understands the content type of the request entity, and the syntax of the
 * request entity is correct, but it was unable to process the contained instructions.
 */
public class UnprocessableContentException extends KnownException {

  public UnprocessableContentException(final String msg) {
    super(msg);
  }

  public UnprocessableContentException(final String msg, final Throwable t) {
    super(msg, t);
  }

  @Override
  public int getHttpCode() {
    return 422;
  }

}
