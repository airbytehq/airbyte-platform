/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.version.AirbyteVersion;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

class ConnectorDatadogSupportHelperTest {

  public static final String CONNECTOR_VERSION = "postgres=2.0.5";
  private final ConnectorDatadogSupportHelper supportHelper = new ConnectorDatadogSupportHelper();

  @Test
  void extractAirbyteVersionFromImageName() {
    final Optional<ImmutablePair<String, AirbyteVersion>> pair =
        supportHelper.extractAirbyteVersionFromImageName(CONNECTOR_VERSION, "=");

    assertTrue(pair.isPresent());
    assertEquals("postgres".compareTo(pair.get().left), 0);
    assertEquals("2.0.5".compareTo(pair.get().right.serialize()), 0);
  }

  @Test
  void connectorVersionCompare() {
    assertTrue(supportHelper.connectorVersionCompare(CONNECTOR_VERSION, "postgres:2.0.5"));
    assertTrue(supportHelper.connectorVersionCompare(CONNECTOR_VERSION, "postgres:2.0.6"));

    assertFalse(supportHelper.connectorVersionCompare(CONNECTOR_VERSION, "postgres:2.0.4"));
    assertFalse(supportHelper.connectorVersionCompare(CONNECTOR_VERSION, "postgres:1.2.8"));

    assertFalse(supportHelper.connectorVersionCompare(CONNECTOR_VERSION, "mysql:2.0.5"));
  }

  @Test
  void verifyInvalidVersionDoesNotThrow() {
    final Optional<ImmutablePair<String, AirbyteVersion>> pair = supportHelper.extractAirbyteVersionFromImageName("custom:v1", ":");
    assertFalse(pair.isPresent());
  }

}
