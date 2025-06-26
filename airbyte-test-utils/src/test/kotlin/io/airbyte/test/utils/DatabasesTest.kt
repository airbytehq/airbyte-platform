/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.zaxxer.hikari.HikariDataSource
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.test.utils.Databases.createDataSource
import io.airbyte.test.utils.Databases.createDslContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

internal class DatabasesTest {
  @Test
  fun testCreatingFromATestContainer() {
    val dataSource = createDataSource(container!!)
    Assertions.assertNotNull(dataSource)
    Assertions.assertEquals(HikariDataSource::class.java, dataSource.javaClass)
    Assertions.assertEquals(10, (dataSource as HikariDataSource).hikariConfigMXBean.maximumPoolSize)
  }

  @Test
  fun testCreatingADslContextFromADataSource() {
    val dialect = SQLDialect.POSTGRES
    val dataSource = createDataSource(container!!)
    val dslContext = createDslContext(dataSource, dialect)
    Assertions.assertNotNull(dslContext)
    Assertions.assertEquals(dialect, dslContext.configuration().dialect())
  }

  companion object {
    private const val DATABASE_NAME = "airbyte_test_database"

    protected var container: PostgreSQLContainer<*>? = null

    @BeforeAll
    @JvmStatic
    fun dbSetup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName(DATABASE_NAME)
          .withUsername("docker")
          .withPassword("docker")
      container!!.start()
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container!!.close()
    }
  }
}
