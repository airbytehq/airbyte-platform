/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.zaxxer.hikari.HikariDataSource;
import io.airbyte.db.instance.DatabaseConstants;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class DatabasesTest {

  private static final String DATABASE_NAME = "airbyte_test_database";

  protected static PostgreSQLContainer<?> container;

  @BeforeAll
  static void dbSetup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName(DATABASE_NAME)
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  @AfterAll
  static void dbDown() {
    container.close();
  }

  @Test
  void testCreatingFromATestContainer() {
    final DataSource dataSource = Databases.createDataSource(container);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(10, ((HikariDataSource) dataSource).getHikariConfigMXBean().getMaximumPoolSize());
  }

  @Test
  void testCreatingADslContextFromADataSource() {
    final SQLDialect dialect = SQLDialect.POSTGRES;
    final DataSource dataSource = Databases.createDataSource(container);
    final DSLContext dslContext = Databases.createDslContext(dataSource, dialect);
    assertNotNull(dslContext);
    assertEquals(dialect, dslContext.configuration().dialect());
  }

}
