/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.zaxxer.hikari.HikariDataSource;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link DataSourceFactory} class.
 */
class DataSourceFactoryTest extends CommonFactoryTest {

  private static final String CONNECT_TIMEOUT = "connectTimeout";

  static String database;
  static String driverClassName;
  static String host;
  static String jdbcUrl;
  static String password;
  static Integer port;
  static String username;

  @BeforeAll
  static void setup() {
    host = container.getHost();
    port = container.getFirstMappedPort();
    database = container.getDatabaseName();
    username = container.getUsername();
    password = container.getPassword();
    driverClassName = container.getDriverClassName();
    jdbcUrl = container.getJdbcUrl();
  }

  @Test
  void testCreatingDataSourceWithConnectionTimeoutSetAboveDefault() {
    final Map<String, String> connectionProperties = Map.of(
        CONNECT_TIMEOUT, "61");
    final DataSource dataSource = DataSourceFactory.create(
        username,
        password,
        driverClassName,
        jdbcUrl,
        connectionProperties);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(61000, ((HikariDataSource) dataSource).getHikariConfigMXBean().getConnectionTimeout());
  }

  @Test
  void testCreatingPostgresDataSourceWithConnectionTimeoutSetBelowDefault() {
    final Map<String, String> connectionProperties = Map.of(CONNECT_TIMEOUT, "30");
    final DataSource dataSource = DataSourceFactory.create(
        username,
        password,
        driverClassName,
        jdbcUrl,
        connectionProperties);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(30000, ((HikariDataSource) dataSource).getHikariConfigMXBean().getConnectionTimeout());
  }

  @Test
  void testCreatingDataSourceWithConnectionTimeoutSetWithZero() {
    final Map<String, String> connectionProperties = Map.of(
        CONNECT_TIMEOUT, "0");
    final DataSource dataSource = DataSourceFactory.create(
        username,
        password,
        driverClassName,
        jdbcUrl,
        connectionProperties);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(Integer.MAX_VALUE, ((HikariDataSource) dataSource).getHikariConfigMXBean().getConnectionTimeout());
  }

  @Test
  void testCreatingPostgresDataSourceWithConnectionTimeoutNotSet() {
    final Map<String, String> connectionProperties = Map.of();
    final DataSource dataSource = DataSourceFactory.create(
        username,
        password,
        driverClassName,
        jdbcUrl,
        connectionProperties);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(10000, ((HikariDataSource) dataSource).getHikariConfigMXBean().getConnectionTimeout());
  }

  @Test
  void testCreatingADataSourceWithJdbcUrl() {
    final DataSource dataSource = DataSourceFactory.create(username, password, driverClassName, jdbcUrl);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(10, ((HikariDataSource) dataSource).getHikariConfigMXBean().getMaximumPoolSize());
  }

  @Test
  void testCreatingADataSourceWithJdbcUrlAndConnectionProperties() {
    final Map<String, String> connectionProperties = Map.of("foo", "bar");

    final DataSource dataSource = DataSourceFactory.create(username, password, driverClassName, jdbcUrl, connectionProperties);
    assertNotNull(dataSource);
    assertEquals(HikariDataSource.class, dataSource.getClass());
    assertEquals(10, ((HikariDataSource) dataSource).getHikariConfigMXBean().getMaximumPoolSize());
  }

  @Test
  void testClosingADataSource() {
    final HikariDataSource dataSource1 = mock(HikariDataSource.class);
    Assertions.assertDoesNotThrow(() -> DataSourceFactory.close(dataSource1));
    verify(dataSource1, times(1)).close();

    final DataSource dataSource2 = mock(DataSource.class);
    Assertions.assertDoesNotThrow(() -> DataSourceFactory.close(dataSource2));

    Assertions.assertDoesNotThrow(() -> DataSourceFactory.close(null));
  }

}
