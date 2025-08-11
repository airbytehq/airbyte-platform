/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConnectorRolloutFinalState
 * <p>
 * Terminal ConnectorRollout state types
 *
 */
public enum ConnectorRolloutFinalState {

  SUCCEEDED("succeeded"),
  FAILED_ROLLED_BACK("failed_rolled_back"),
  CANCELED("canceled");

  private final String value;
  private final static Map<String, ConnectorRolloutFinalState> CONSTANTS = new HashMap<String, ConnectorRolloutFinalState>();

  static {
    for (ConnectorRolloutFinalState c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConnectorRolloutFinalState(String value) {
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
  public static ConnectorRolloutFinalState fromValue(String value) {
    ConnectorRolloutFinalState constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
