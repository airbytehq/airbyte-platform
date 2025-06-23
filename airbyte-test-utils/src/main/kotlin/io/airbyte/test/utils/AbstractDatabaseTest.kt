/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.test.utils.Databases.createDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.PostgreSQLContainer
import java.io.IOException
import javax.sql.DataSource

abstract class AbstractDatabaseTest {
  protected var database: Database? = null
  protected var dataSource: DataSource? = null
  protected var dslContext: DSLContext? = null

  @BeforeEach
  @Throws(IOException::class, DatabaseInitializationException::class)
  fun setup() {
    dataSource = createDataSource(container!!)
    dslContext = create(dataSource!!, SQLDialect.POSTGRES)
    database = createDatabase(dataSource!!, dslContext!!)
  }

  @AfterEach
  @Throws(Exception::class)
  fun tearDown() {
    close(dataSource)
  }

  /**
   * Create a [Database]. The downstream implementation should call
   * [DatabaseMigrator.migrate] if necessary.
   *
   * @param dataSource The [DataSource] used to access the database.
   * @param dslContext The [DSLContext] used to execute queries.
   * @return an initialized [Database] instance.
   */
  @Throws(IOException::class, DatabaseInitializationException::class)
  abstract fun createDatabase(
    dataSource: DataSource,
    dslContext: DSLContext,
  ): Database

  companion object {
    @JvmStatic
    protected var container: PostgreSQLContainer<*>? = null

    @BeforeAll
    @JvmStatic
    fun dbSetup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName("airbyte")
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
