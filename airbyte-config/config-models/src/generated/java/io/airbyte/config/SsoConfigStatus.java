/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * SsoConfigStatus
 * <p>
 * SSO Configuration status types
 *
 */
public enum SsoConfigStatus {

  DRAFT("draft"),
  ACTIVE("active");

  private final String value;
  private final static Map<String, SsoConfigStatus> CONSTANTS = new HashMap<String, SsoConfigStatus>();

  static {
    for (SsoConfigStatus c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  SsoConfigStatus(String value) {
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
  public static SsoConfigStatus fromValue(String value) {
    SsoConfigStatus constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
