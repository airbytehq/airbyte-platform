/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

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
