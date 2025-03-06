/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.DatabaseMigrator;
import java.io.IOException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

public abstract class AbstractDatabaseTest {

  protected static PostgreSQLContainer<?> container;

  @BeforeAll
  public static void dbSetup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  @AfterAll
  public static void dbDown() {
    container.close();
  }

  protected Database database;
  protected DataSource dataSource;
  protected DSLContext dslContext;

  @BeforeEach
  public void setup() throws IOException, DatabaseInitializationException {
    dataSource = Databases.createDataSource(container);
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    database = getDatabase(dataSource, dslContext);
  }

  @AfterEach
  void tearDown() throws Exception {
    DataSourceFactory.close(dataSource);
  }

  /**
   * Create a {@link Database}. The downstream implementation should call
   * {@link DatabaseMigrator#migrate} if necessary.
   *
   * @param dataSource The {@link DataSource} used to access the database.
   * @param dslContext The {@link DSLContext} used to execute queries.
   * @return an initialized {@link Database} instance.
   */
  public abstract Database getDatabase(DataSource dataSource, DSLContext dslContext) throws IOException, DatabaseInitializationException;

  public DataSource getDataSource() {
    return dataSource;
  }

  public DSLContext getDslContext() {
    return dslContext;
  }

}
