/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ResourceScope
 * <p>
 * Scope of a given resource
 *
 */
public enum ResourceScope {

  ORGANIZATION("organization"),
  WORKSPACE("workspace"),
  GLOBAL("global");

  private final String value;
  private final static Map<String, ResourceScope> CONSTANTS = new HashMap<String, ResourceScope>();

  static {
    for (ResourceScope c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ResourceScope(String value) {
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
  public static ResourceScope fromValue(String value) {
    ResourceScope constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
