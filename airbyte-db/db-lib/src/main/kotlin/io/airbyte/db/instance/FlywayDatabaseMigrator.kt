/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance

import io.airbyte.db.Database
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationInfo
import org.flywaydb.core.api.output.BaselineResult
import org.flywaydb.core.api.output.MigrateResult
import org.jooq.DSLContext

private val log = KotlinLogging.logger {}

/**
 * Set of schemas used to filter the schema dump generated via the jOOQ DDL.
 */
private val SCHEMAS = setOf("public")

private const val DISCLAIMER =
  """// The content of the file is just to have a basic idea of the current state of the database and is not fully accurate.
// It is also not used by any piece of code to generate anything.
// It doesn't contain the enums created in the database and the default values might also be buggy.

"""

open class FlywayDatabaseMigrator(
  val database: Database,
  val flyway: Flyway,
) : DatabaseMigrator {
  override fun migrate(): MigrateResult =
    flyway
      .migrate()
      .also { it.warnings.forEach { msg -> log.warn { msg } } }

  override fun list(): List<MigrationInfo> = flyway.info().all().asList()

  override fun getLatestMigration(): MigrationInfo? = flyway.info().current()

  override fun createBaseline(): BaselineResult? =
    flyway
      .baseline()
      .also { it.warnings.forEach { msg -> log.warn { msg } } }

  override fun dumpSchema(): String =
    DISCLAIMER +
      database.query { ctx: DSLContext ->
        ctx
          .meta()
          .filterSchemas { SCHEMAS.contains(it.name) }
          .ddl()
          .queries()
          .map { "$it;" }
          .filter { !it.startsWith("create schema") }
          .joinToString(separator = "\n")
      }
}
