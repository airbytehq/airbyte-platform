/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * DestinationSyncMode
 * <p>
 * destination sync modes.
 */
public enum DestinationSyncMode {

  APPEND("append"),
  OVERWRITE("overwrite"),
  APPEND_DEDUP("append_dedup"),
  OVERWRITE_DEDUP("overwrite_dedup");

  private final String value;
  private static final Map<String, DestinationSyncMode> CONSTANTS = new HashMap<String, DestinationSyncMode>();

  static {
    for (DestinationSyncMode c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private DestinationSyncMode(String value) {
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
  public static DestinationSyncMode fromValue(String value) {
    DestinationSyncMode constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
