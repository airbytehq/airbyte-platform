/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConfigResourceType
 * <p>
 * ScopedConfiguration resource types
 *
 */
public enum ConfigResourceType {

  ACTOR_DEFINITION("actor_definition"),
  USER("user"),
  WORKSPACE("workspace"),
  CONNECTION("connection"),
  SOURCE("source"),
  DESTINATION("destination");

  private final String value;
  private final static Map<String, ConfigResourceType> CONSTANTS = new HashMap<String, ConfigResourceType>();

  static {
    for (ConfigResourceType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConfigResourceType(String value) {
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
  public static ConfigResourceType fromValue(String value) {
    ConfigResourceType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
