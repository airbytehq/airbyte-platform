/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * StateType
 * <p>
 * State Types
 *
 */
public enum StateType {

  GLOBAL("global"),
  STREAM("stream"),
  LEGACY("legacy");

  private final String value;
  private final static Map<String, StateType> CONSTANTS = new HashMap<String, StateType>();

  static {
    for (StateType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private StateType(String value) {
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
  public static StateType fromValue(String value) {
    StateType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
