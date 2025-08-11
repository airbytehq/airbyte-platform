/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConfigOriginType
 * <p>
 * ScopedConfiguration origin types
 *
 */
public enum ConfigOriginType {

  USER("user"),
  BREAKING_CHANGE("breaking_change"),
  CONNECTOR_ROLLOUT("connector_rollout");

  private final String value;
  private final static Map<String, ConfigOriginType> CONSTANTS = new HashMap<String, ConfigOriginType>();

  static {
    for (ConfigOriginType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConfigOriginType(String value) {
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
  public static ConfigOriginType fromValue(String value) {
    ConfigOriginType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
