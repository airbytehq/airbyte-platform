/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.create
import org.jooq.SQLDialect
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

/**
 * Test suite for the [DSLContextFactory] class.
 */
internal class DSLContextFactoryTest : CommonFactoryTest() {
  @Test
  fun testCreatingADslContext() {
    val dataSource =
      create(
        username = container.username,
        password = container.password,
        driverClassName = container.driverClassName,
        jdbcConnectionString = container.jdbcUrl,
      )
    val dialect = SQLDialect.POSTGRES
    val dslContext = create(dataSource, dialect)
    assertNotNull(dslContext)
    assertEquals(dialect, dslContext.configuration().dialect())
  }

  @Test
  fun testCreatingADslContextWithIndividualConfiguration() {
    val dialect = SQLDialect.POSTGRES
    val dslContext =
      create(
        username = container.username,
        password = container.password,
        driverClassName = container.driverClassName,
        jdbcConnectionString = container.jdbcUrl,
        dialect = dialect,
      )
    assertNotNull(dslContext)
    assertEquals(dialect, dslContext.configuration().dialect())
  }
}
