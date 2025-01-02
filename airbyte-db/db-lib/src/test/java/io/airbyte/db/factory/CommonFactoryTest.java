/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory;

import io.airbyte.db.instance.DatabaseConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Common test suite for the classes found in the {@code io.airbyte.db.factory} package.
 */
class CommonFactoryTest {

  private static final String DATABASE_NAME = "airbyte_test_database";

  protected static PostgreSQLContainer<?> container;

  @BeforeAll
  public static void dbSetup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName(DATABASE_NAME)
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  @AfterAll
  public static void dbDown() {
    container.close();
  }

}
