/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.json.Jsons;
import org.junit.jupiter.api.Test;

class VersionTest {

  private static final Version LOWER_VERSION = new Version("1.0.0");
  private static final Version HIGHER_VERSION = new Version("1.2.3");

  @Test
  void testJsonSerializationDeserialization() {
    final String jsonString = """
                              {"version": "1.2.3"}
                              """;
    final Version expectedVersion = new Version("1.2.3");

    final Version deserializedVersion = Jsons.deserialize(jsonString, Version.class);
    assertEquals(expectedVersion, deserializedVersion);

    final Version deserializedVersionLoop = Jsons.deserialize(Jsons.serialize(deserializedVersion), Version.class);
    assertEquals(expectedVersion, deserializedVersionLoop);
  }

  @Test
  void testGreaterThanOrEqualTo() {
    assertTrue(LOWER_VERSION.greaterThanOrEqualTo(LOWER_VERSION));
    assertTrue(HIGHER_VERSION.greaterThanOrEqualTo(LOWER_VERSION));
    assertFalse(LOWER_VERSION.greaterThanOrEqualTo(HIGHER_VERSION));
  }

  @Test
  void testGreaterThan() {
    assertFalse(LOWER_VERSION.greaterThan(LOWER_VERSION));
    assertTrue(HIGHER_VERSION.greaterThan(LOWER_VERSION));
    assertFalse(LOWER_VERSION.greaterThan(HIGHER_VERSION));
  }

  @Test
  void testLessThan() {
    assertFalse(LOWER_VERSION.lessThan(LOWER_VERSION));
    assertFalse(HIGHER_VERSION.lessThan(LOWER_VERSION));
    assertTrue(LOWER_VERSION.lessThan(HIGHER_VERSION));
  }

  @Test
  void testLessThanOrEqualTo() {
    assertTrue(LOWER_VERSION.lessThanOrEqualTo(LOWER_VERSION));
    assertFalse(HIGHER_VERSION.lessThanOrEqualTo(LOWER_VERSION));
    assertTrue(LOWER_VERSION.lessThanOrEqualTo(HIGHER_VERSION));
  }

}
