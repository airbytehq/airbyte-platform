/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ReleaseStage
 * <p>
 * enum that describes a connector's release stage
 *
 */
public enum ReleaseStage {

  ALPHA("alpha"),
  BETA("beta"),
  GENERALLY_AVAILABLE("generally_available"),
  CUSTOM("custom");

  private final String value;
  private final static Map<String, ReleaseStage> CONSTANTS = new HashMap<String, ReleaseStage>();

  static {
    for (ReleaseStage c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ReleaseStage(String value) {
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
  public static ReleaseStage fromValue(String value) {
    ReleaseStage constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
