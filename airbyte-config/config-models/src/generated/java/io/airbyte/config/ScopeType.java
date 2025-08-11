/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ScopeType
 * <p>
 * Scope type of resource id
 *
 */
public enum ScopeType {

  ORGANIZATION("organization"),
  WORKSPACE("workspace");

  private final String value;
  private final static Map<String, ScopeType> CONSTANTS = new HashMap<String, ScopeType>();

  static {
    for (ScopeType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ScopeType(String value) {
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
  public static ScopeType fromValue(String value) {
    ScopeType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
