/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development

import org.flywaydb.core.api.MigrationInfo
import org.flywaydb.core.api.output.MigrateOutput
import org.flywaydb.core.api.output.MigrateResult
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.DefaultDSLContext
import org.jooq.impl.SQLDataType
import java.sql.Date

/**
 * This class formats the Flyway outputs so that it is easier to inspect them and debug the
 * migration.
 */
object FlywayFormatter {
  private val CTX: DSLContext = DefaultDSLContext(SQLDialect.DEFAULT)

  /**
   * Format the [DatabaseMigrator.list] output.
   */
  @JvmStatic
  fun formatMigrationInfoList(migrationInfoList: List<MigrationInfo>): String {
    val type = DSL.field("Type", SQLDataType.VARCHAR)
    val version = DSL.field("Version", SQLDataType.VARCHAR)
    val description = DSL.field("Description", SQLDataType.VARCHAR)
    val state = DSL.field("State", SQLDataType.VARCHAR)
    val migratedAt = DSL.field("MigratedAt", SQLDataType.DATE)
    val result = CTX.newResult(type, version, description, state, migratedAt)

    migrationInfoList.forEach { info: MigrationInfo ->
      result.add(
        CTX.newRecord(type, version, description, state, migratedAt).values(
          info.type.name(),
          info.version.toString(),
          info.description,
          info.state.displayName,
          if (info.installedOn == null) null else Date(info.installedOn.time),
        ),
      )
    }

    return result.format()
  }

  private fun formatMigrationOutputList(migrationOutputList: List<MigrateOutput>): String {
    val type = DSL.field("Type", SQLDataType.VARCHAR)
    val version = DSL.field("Version", SQLDataType.VARCHAR)
    val description = DSL.field("Description", SQLDataType.VARCHAR)
    val script = DSL.field("Script", SQLDataType.VARCHAR)
    val result = CTX.newResult(type, version, description, script)

    migrationOutputList.forEach { output: MigrateOutput ->
      result.add(
        CTX.newRecord(type, version, description, script).values(
          "${output.type} ${output.category}",
          output.version,
          output.description,
          output.filepath,
        ),
      )
    }
    return result.format()
  }

  /**
   * Format the [DatabaseMigrator.migrate] output.
   */
  @JvmStatic
  fun formatMigrationResult(result: MigrateResult): String =
    """
    Version: ${result.initialSchemaVersion} -> ${result.targetSchemaVersion}
    Migrations executed: ${result.migrationsExecuted}
    ${formatMigrationOutputList(result.migrations)}
    """.trimIndent()
}
