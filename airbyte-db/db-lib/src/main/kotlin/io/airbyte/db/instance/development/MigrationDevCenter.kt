/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development

import com.google.common.annotations.VisibleForTesting
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.factory.DataSourceFactory.create
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.FlywayDatabaseMigrator
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrationDevCenter
import io.airbyte.db.instance.development.MigrationDevHelper.createNextMigrationFile
import io.airbyte.db.instance.development.MigrationDevHelper.dumpSchema
import io.airbyte.db.instance.development.MigrationDevHelper.runLastMigration
import io.airbyte.db.instance.jobs.JobsDatabaseMigrationDevCenter
import org.flywaydb.core.Flyway
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.ext.ScriptUtils
import org.testcontainers.jdbc.JdbcDatabaseDelegate
import javax.sql.DataSource

private enum class Db {
  CONFIGS,
  JOBS,
}

private enum class Command {
  CREATE,
  MIGRATE,
  DUMP_SCHEMA,
}

/**
 * Helper class for migration development. See README for details.
 */
abstract class MigrationDevCenter protected constructor(
  private val dbIdentifier: String,
  private val schemaDumpFile: String,
  private val initialScript: String,
) {
  private fun createContainer(): PostgreSQLContainer<*> {
    val container =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker")
        .apply { start() }

    val containerDelegate = JdbcDatabaseDelegate(container, "")
    ScriptUtils.runInitScript(containerDelegate, initialScript)
    return container
  }

  protected abstract fun getMigrator(
    database: Database,
    flyway: Flyway,
  ): FlywayDatabaseMigrator

  protected abstract fun getFlyway(dataSource: DataSource): Flyway

  private fun createMigration(description: String) {
    try {
      createContainer().use { container ->
        val dataSource = createDataSource(container)
        try {
          val dslContext = create(dataSource = dataSource)
          val flyway = getFlyway(dataSource)
          val database = Database(dslContext)
          val migrator = getMigrator(database, flyway)
          createNextMigrationFile(dbIdentifier = dbIdentifier, migrator = migrator, description = description)
        } finally {
          close(dataSource)
        }
      }
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  private fun runLastMigration() {
    try {
      createContainer().use { container ->
        val dataSource = createDataSource(container)
        try {
          val dslContext = create(dataSource = dataSource)
          val flyway = getFlyway(dataSource)
          val database = Database(dslContext)
          val fullMigrator = getMigrator(database, flyway)
          val devDatabaseMigrator = DevDatabaseMigrator(fullMigrator)
          runLastMigration(devDatabaseMigrator)
          val schema = fullMigrator.dumpSchema()
          dumpSchema(schema, schemaDumpFile, false)
        } finally {
          close(dataSource)
        }
      }
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  /**
   * Dump schema.
   *
   * @param persistToFile file to persist to
   * @return schema
   */
  @VisibleForTesting
  fun dumpSchema(persistToFile: Boolean = false): String {
    try {
      createContainer().use { container ->
        val dataSource = createDataSource(container)
        try {
          val dslContext = create(dataSource = dataSource)
          val flyway = getFlyway(dataSource)
          val database = Database(dslContext)
          val migrator = getMigrator(database, flyway)
          migrator.migrate()
          val schema = migrator.dumpSchema()

          if (persistToFile) {
            dumpSchema(schema, schemaDumpFile, true)
          }

          return schema
        } finally {
          close(dataSource)
        }
      }
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  private fun createDataSource(container: PostgreSQLContainer<*>): DataSource =
    create(
      username = container.username,
      password = container.password,
      driverClassName = container.driverClassName,
      jdbcConnectionString = container.jdbcUrl,
    )

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      val devCenter: MigrationDevCenter =
        when (Db.valueOf(args[0].uppercase())) {
          Db.CONFIGS -> ConfigsDatabaseMigrationDevCenter()
          Db.JOBS -> JobsDatabaseMigrationDevCenter()
        }

      when (Command.valueOf(args[1].uppercase())) {
        Command.CREATE -> {
          if (args.size != 3) {
            throw IllegalArgumentException("missing description parameter for new migration ")
          }
          devCenter.createMigration(args[2])
        }
        Command.MIGRATE -> devCenter.runLastMigration()
        Command.DUMP_SCHEMA -> devCenter.dumpSchema(true)
      }
    }
  }
}
