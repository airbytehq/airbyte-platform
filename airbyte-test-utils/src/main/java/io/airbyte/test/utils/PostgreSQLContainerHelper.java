/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import java.io.IOException;
import java.util.UUID;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Helper for doing common postgres test container setup.
 */
public class PostgreSQLContainerHelper {

  /**
   * Setup database with a setup script.
   *
   * @param file file to execute
   * @param db database
   */
  public static void runSqlScript(final MountableFile file, final PostgreSQLContainer db) {
    try {
      final String scriptPath = "/etc/" + UUID.randomUUID() + ".sql";
      db.copyFileToContainer(file, scriptPath);
      db.execInContainer(
          "psql", "-d", db.getDatabaseName(), "-U", db.getUsername(), "-a", "-f", scriptPath);

    } catch (final InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

}
