/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * A sync is running for this connection.
 */
public class SyncIsRunningException extends KnownException {

  public SyncIsRunningException(final String message) {
    super(message);
  }

  public SyncIsRunningException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return 423;
  }

}
