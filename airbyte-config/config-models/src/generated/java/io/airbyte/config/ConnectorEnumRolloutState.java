/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ConnectorEnumRolloutState
 * <p>
 * ConnectorRollout state types
 *
 */
public enum ConnectorEnumRolloutState {

  INITIALIZED("initialized"),
  WORKFLOW_STARTED("workflow_started"),
  IN_PROGRESS("in_progress"),
  PAUSED("paused"),
  FINALIZING("finalizing"),
  SUCCEEDED("succeeded"),
  ERRORED("errored"),
  FAILED_ROLLED_BACK("failed_rolled_back"),
  CANCELED("canceled");

  private final String value;
  private final static Map<String, ConnectorEnumRolloutState> CONSTANTS = new HashMap<String, ConnectorEnumRolloutState>();

  static {
    for (ConnectorEnumRolloutState c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ConnectorEnumRolloutState(String value) {
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
  public static ConnectorEnumRolloutState fromValue(String value) {
    ConnectorEnumRolloutState constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
