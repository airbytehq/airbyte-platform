/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConnectorEnumRolloutStrategy
 * <p>
 * Connector Rollout strategy types
 *
 */
public enum ConnectorEnumRolloutStrategy {

  MANUAL("manual"),
  AUTOMATED("automated"),
  OVERRIDDEN("overridden");

  private final String value;
  private final static Map<String, ConnectorEnumRolloutStrategy> CONSTANTS = new HashMap<String, ConnectorEnumRolloutStrategy>();

  static {
    for (ConnectorEnumRolloutStrategy c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConnectorEnumRolloutStrategy(String value) {
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
  public static ConnectorEnumRolloutStrategy fromValue(String value) {
    ConnectorEnumRolloutStrategy constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
