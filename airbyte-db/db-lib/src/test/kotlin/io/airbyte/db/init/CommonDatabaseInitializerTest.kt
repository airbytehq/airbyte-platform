/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.factory.DataSourceFactory.create
import io.airbyte.db.instance.DatabaseConstants
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.PostgreSQLContainer
import javax.sql.DataSource

/**
 * Common test setup for database initialization tests.
 */
internal open class CommonDatabaseInitializerTest {
  internal lateinit var container: PostgreSQLContainer<*>
  internal lateinit var dataSource: DataSource
  internal lateinit var dslContext: DSLContext

  @BeforeEach
  fun setup() {
    container = PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION).apply { start() }

    dataSource =
      create(
        username = container.username,
        password = container.password,
        driverClassName = container.driverClassName,
        jdbcConnectionString = container.jdbcUrl,
      )
    dslContext = create(dataSource = dataSource, dialect = SQLDialect.POSTGRES)
  }

  @AfterEach
  fun cleanup() {
    close(dataSource)
    container.stop()
  }
}
