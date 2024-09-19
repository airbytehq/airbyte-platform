/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Date;
import java.util.Optional;

/**
 * An immutable representation of an Airbyte License.
 */
public record AirbyteLicense(LicenseType type, Optional<Date> expirationDate, Optional<Integer> maxNodes, Optional<Integer> maxEditors) {

  public enum LicenseType {

    PRO("pro"),
    INVALID("invalid");

    private final String value;

    LicenseType(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static LicenseType fromValue(String value) {
      for (LicenseType b : LicenseType.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }

  }

}
