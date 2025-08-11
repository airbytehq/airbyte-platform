/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;

/**
 * InvitationStatus
 * <p>
 * Userinvitation status enum
 *
 */
public enum InvitationStatus {

  PENDING("pending"),
  ACCEPTED("accepted"),
  CANCELLED("cancelled"),
  DECLINED("declined"),
  EXPIRED("expired");

  private final String value;
  private final static Map<String, InvitationStatus> CONSTANTS = new HashMap<String, InvitationStatus>();

  static {
    for (InvitationStatus c : values()) {
      CONSTANTS.put(c.value, c);
    }
  }

  private InvitationStatus(String value) {
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
  public static InvitationStatus fromValue(String value) {
    InvitationStatus constant = CONSTANTS.get(value);
    if (constant == null) {
      throw new IllegalArgumentException(value);
    } else {
      return constant;
    }
  }

}
