/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link DSLContextFactory} class.
 */
class DSLContextFactoryTest extends CommonFactoryTest {

  @Test
  void testCreatingADslContext() {
    final DataSource dataSource =
        DataSourceFactory.create(container.getUsername(), container.getPassword(), container.getDriverClassName(), container.getJdbcUrl());
    final SQLDialect dialect = SQLDialect.POSTGRES;
    final DSLContext dslContext = DSLContextFactory.create(dataSource, dialect);
    assertNotNull(dslContext);
    assertEquals(dialect, dslContext.configuration().dialect());
  }

  @Test
  void testCreatingADslContextWithIndividualConfiguration() {
    final SQLDialect dialect = SQLDialect.POSTGRES;
    final DSLContext dslContext = DSLContextFactory.create(
        container.getUsername(),
        container.getPassword(),
        container.getDriverClassName(),
        container.getJdbcUrl(),
        dialect);
    assertNotNull(dslContext);
    assertEquals(dialect, dslContext.configuration().dialect());
  }

}
