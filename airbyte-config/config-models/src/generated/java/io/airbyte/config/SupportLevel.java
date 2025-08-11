/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * SupportLevel
 * <p>
 * enum that describes a connector's support level
 *
 */
public enum SupportLevel {

  COMMUNITY("community"),
  CERTIFIED("certified"),
  ARCHIVED("archived"),
  NONE("none");

  private final String value;
  private final static Map<String, SupportLevel> CONSTANTS = new HashMap<String, SupportLevel>();

  static {
    for (SupportLevel c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private SupportLevel(String value) {
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
  public static SupportLevel fromValue(String value) {
    SupportLevel constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
