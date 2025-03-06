/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development;

import static io.airbyte.db.instance.development.MigrationDevHelper.AIRBYTE_VERSION_ENV_VAR;
import static io.airbyte.db.instance.development.MigrationDevHelper.VERSION_ENV_VAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.commons.version.AirbyteVersion;
import java.util.Optional;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@SuppressWarnings({"PMD.AvoidUsingHardCodedIP", "PMD.JUnitTestsShouldIncludeAssert"})
class MigrationDevHelperTest {

  private static final String VERSION_0113_ALPHA = "0.11.3-alpha";

  @SetEnvironmentVariable(key = AIRBYTE_VERSION_ENV_VAR,
                          value = VERSION_0113_ALPHA)
  @Test
  void testGetCurrentAirbyteVersion() {
    // Test that this method will not throw any exception.
    assertEquals(VERSION_0113_ALPHA, MigrationDevHelper.getCurrentAirbyteVersion().serialize());
  }

  @SetEnvironmentVariable(key = VERSION_ENV_VAR,
                          value = "dev")
  @Test
  void testGetCurrentAirbyteVersionFromFallbackEnv() {
    // Test that this method will not throw any exception.
    assertEquals("dev", MigrationDevHelper.getCurrentAirbyteVersion().serialize());
  }

  @ClearEnvironmentVariable(key = AIRBYTE_VERSION_ENV_VAR)
  @ClearEnvironmentVariable(key = VERSION_ENV_VAR)
  @Test
  void testGetCurrentAirbyteVersionFailure() {
    assertThrows(IllegalStateException.class, () -> MigrationDevHelper.getCurrentAirbyteVersion().toString());
  }

  @Test
  void testGetAirbyteVersion() {
    final MigrationVersion migrationVersion = MigrationVersion.fromVersion("0.11.3.010");
    final AirbyteVersion airbyteVersion = MigrationDevHelper.getAirbyteVersion(migrationVersion);
    assertEquals("0.11.3", airbyteVersion.serialize());
  }

  @Test
  void testFormatAirbyteVersion() {
    final AirbyteVersion airbyteVersion = new AirbyteVersion(VERSION_0113_ALPHA);
    assertEquals("0_11_3", MigrationDevHelper.formatAirbyteVersion(airbyteVersion));
  }

  @Test
  void testGetMigrationId() {
    final MigrationVersion migrationVersion = MigrationVersion.fromVersion("0.11.3.010");
    assertEquals("010", MigrationDevHelper.getMigrationId(migrationVersion));
  }

  @Test
  void testGetNextMigrationVersion() {
    // Migration version does not exist
    assertEquals("0.11.3.001", MigrationDevHelper.getNextMigrationVersion(
        new AirbyteVersion(VERSION_0113_ALPHA),
        Optional.empty()).getVersion());

    // Airbyte version is greater
    assertEquals("0.11.3.001", MigrationDevHelper.getNextMigrationVersion(
        new AirbyteVersion(VERSION_0113_ALPHA),
        Optional.of(MigrationVersion.fromVersion("0.10.9.003"))).getVersion());

    // Airbyte version is equal to migration version
    assertEquals("0.11.3.004", MigrationDevHelper.getNextMigrationVersion(
        new AirbyteVersion(VERSION_0113_ALPHA),
        Optional.of(MigrationVersion.fromVersion("0.11.3.003"))).getVersion());

    // Migration version is greater
    assertEquals("0.11.3.004", MigrationDevHelper.getNextMigrationVersion(
        new AirbyteVersion("0.9.17-alpha"),
        Optional.of(MigrationVersion.fromVersion("0.11.3.003"))).getVersion());
  }

}
