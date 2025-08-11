/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * ResourceRequirementsType
 * <p>
 * ResourceRequirements types.
 *
 */
public enum ResourceRequirementsType {

  DESTINATION("Destination"),
  ORCHESTRATOR("Orchestrator"),
  SOURCE("Source");

  private final String value;
  private final static Map<String, ResourceRequirementsType> CONSTANTS = new HashMap<String, ResourceRequirementsType>();

  static {
    for (ResourceRequirementsType c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private ResourceRequirementsType(String value) {
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
  public static ResourceRequirementsType fromValue(String value) {
    ResourceRequirementsType constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
