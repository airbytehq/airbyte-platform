/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json;

/**
 * A JSON object was not valid against a given JSONSchema.
 */
public class JsonValidationException extends Exception {

  public JsonValidationException(final String message) {
    super(message);
  }

  public JsonValidationException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
