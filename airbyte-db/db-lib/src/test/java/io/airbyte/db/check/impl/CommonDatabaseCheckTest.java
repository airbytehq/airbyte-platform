/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check.impl;

import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.instance.DatabaseConstants;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Common test setup for database availability check tests.
 */
class CommonDatabaseCheckTest {

  protected static final long TIMEOUT_MS = 500L;

  protected PostgreSQLContainer<?> container;

  protected DataSource dataSource;

  protected DSLContext dslContext;

  @BeforeEach
  void setup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION);
    container.start();

    dataSource = DataSourceFactory.create(container.getUsername(), container.getPassword(), container.getDriverClassName(), container.getJdbcUrl());
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
  }

  @SuppressWarnings("PMD.SignatureDeclareThrowsException")
  @AfterEach
  void cleanup() throws Exception {
    DataSourceFactory.close(dataSource);
    container.stop();
  }

}
