/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

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
 * Common test setup for database availability check tests.
 */
internal open class CommonsDatabaseCheckTest {
  protected lateinit var container: PostgreSQLContainer<*>
  protected lateinit var dataSource: DataSource
  protected lateinit var dslContext: DSLContext

  @BeforeEach
  fun setup() {
    container = PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION).apply { start() }

    dataSource = create(container.username, container.password, container.driverClassName, container.jdbcUrl)
    dslContext = create(dataSource, SQLDialect.POSTGRES)
  }

  @AfterEach
  fun cleanup() {
    close(dataSource)
    container.stop()
  }

  companion object {
    internal const val TIMEOUT_MS: Long = 500L
  }
}
