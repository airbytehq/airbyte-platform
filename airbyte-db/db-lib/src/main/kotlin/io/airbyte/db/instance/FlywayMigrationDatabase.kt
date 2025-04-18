/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance

import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.factory.DataSourceFactory.create
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.init.DatabaseInitializationException
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.meta.postgres.PostgresDatabase
import org.jooq.tools.jdbc.JDBCUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PostgreSQLContainer
import java.io.IOException
import java.sql.Connection
import javax.sql.DataSource

/**
 * Custom database for jOOQ code generation. It performs the following operations:
 *
 *  * Run Flyway migration.
 *  * Dump the database schema.
 *  * Create a connection for jOOQ code generation.
 *
 * Reference: https://github.com/sabomichal/jooq-meta-postgres-flyway
 */
abstract class FlywayMigrationDatabase : PostgresDatabase() {
  private var connection: Connection? = null

  private var dataSource: DataSource? = null

  private var dslContext: DSLContext? = null

  @Throws(IOException::class)
  protected abstract fun getDatabase(dslContext: DSLContext): Database

  protected abstract fun getDatabaseMigrator(
    database: Database,
    flyway: Flyway,
  ): DatabaseMigrator

  protected abstract val installedBy: String?

  protected abstract val dbIdentifier: String?

  protected abstract val migrationFileLocations: Array<String>

  @Throws(DatabaseInitializationException::class, IOException::class)
  protected abstract fun initializeDatabase(dslContext: DSLContext)

  override fun create0(): DSLContext = DSL.using(internalConnection, SQLDialect.POSTGRES)

  protected val internalConnection: Connection?
    get() {
      return connection ?: try {
        createInternalConnection()
      } catch (e: Exception) {
        throw RuntimeException("Failed to launch postgres container and run migration", e)
      }
    }

  private fun createInternalConnection(): Connection? {
    val dockerImage = properties.getProperty("dockerImage")?.takeIf { it.isNotBlank() } ?: DatabaseConstants.DEFAULT_DATABASE_VERSION

    val container =
      PostgreSQLContainer(dockerImage)
        .withDatabaseName("jooq_airbyte_configs")
        .withUsername("jooq_generator")
        .withPassword("jooq_generator")
    container.start()

    dataSource = create(container.username, container.password, container.driverClassName, container.jdbcUrl)
    dslContext = create(dataSource!!, SQLDialect.POSTGRES)

    initializeDatabase(dslContext!!)

    val flyway = create(dataSource, installedBy, dbIdentifier, *migrationFileLocations)
    val database = getDatabase(dslContext!!)
    val migrator = getDatabaseMigrator(database, flyway)
    migrator.migrate()

    connection = dataSource!!.connection
    setConnection(connection)
    return connection
  }

  override fun close() {
    JDBCUtils.safeClose(connection)
    connection = null
    try {
      close(dataSource)
    } catch (e: Exception) {
      LOGGER.warn("Unable to close data source.", e)
    }
    super.close()
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(FlywayMigrationDatabase::class.java)
  }
}
