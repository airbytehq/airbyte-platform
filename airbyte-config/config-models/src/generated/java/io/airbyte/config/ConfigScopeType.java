/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConfigScopeType
 * <p>
 * ScopedConfiguration scope types
 *
 */
public enum ConfigScopeType {

  ORGANIZATION("organization"),
  WORKSPACE("workspace"),
  ACTOR("actor");

  private final String value;
  private final static Map<String, ConfigScopeType> CONSTANTS = new HashMap<String, ConfigScopeType>();

  static {
    for (ConfigScopeType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConfigScopeType(String value) {
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
  public static ConfigScopeType fromValue(String value) {
    ConfigScopeType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
