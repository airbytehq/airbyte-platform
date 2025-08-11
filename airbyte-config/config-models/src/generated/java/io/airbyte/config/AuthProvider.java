/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * AuthProvider
 * <p>
 * enum that describes the different types of auth providers that the platform supports.
 *
 */
public enum AuthProvider {

  AIRBYTE("airbyte"),
  GOOGLE_IDENTITY_PLATFORM("google_identity_platform"),
  KEYCLOAK("keycloak");

  private final String value;
  private final static Map<String, AuthProvider> CONSTANTS = new HashMap<String, AuthProvider>();

  static {
    for (AuthProvider c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private AuthProvider(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return this.value;
  }

  @JsonValue
  public String value() {
    return this.value;
  }

  @JsonCreator
  public static AuthProvider fromValue(String value) {
    AuthProvider constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
