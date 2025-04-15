/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development

import org.flywaydb.core.api.MigrationInfo
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
internal object FlywayFormatter {
  private val CTX: DSLContext = DefaultDSLContext(SQLDialect.DEFAULT)

  /**
   * Format the [DatabaseMigrator.list] output.
   */
  fun formatMigrationInfoList(migrationInfos: List<MigrationInfo>): String {
    val type = DSL.field("Type", SQLDataType.VARCHAR)
    val version = DSL.field("Version", SQLDataType.VARCHAR)
    val description = DSL.field("Description", SQLDataType.VARCHAR)
    val state = DSL.field("State", SQLDataType.VARCHAR)
    val migratedAt = DSL.field("MigratedAt", SQLDataType.DATE)
    val result = CTX.newResult(type, version, description, state, migratedAt)

    migrationInfos.forEach { info: MigrationInfo ->
      result.add(
        CTX.newRecord(type, version, description, state, migratedAt).values(
          info.type.name(),
          info.version.toString(),
          info.description,
          info.state.displayName,
          info.installedOn?.let { Date(it.time) },
        ),
      )
    }

    return result.format()
  }

  /**
   * Format the [MigrateResult] output.
   */
  fun formatMigrationResult(result: MigrateResult): String {
    val type = DSL.field("Type", SQLDataType.VARCHAR)
    val version = DSL.field("Version", SQLDataType.VARCHAR)
    val description = DSL.field("Description", SQLDataType.VARCHAR)
    val script = DSL.field("Script", SQLDataType.VARCHAR)
    val newResult = CTX.newResult(type, version, description, script)
    result.migrations.forEach {
      newResult.add(
        CTX
          .newRecord(type, version, description, script)
          .values("${it.type} ${it.category}", it.version, it.description, it.filepath),
      )
    }
    val migrationOutput = newResult.format()

    return """
      Version: ${result.initialSchemaVersion} -> ${result.targetSchemaVersion}
      Migrations executed: ${result.migrationsExecuted}
      $migrationOutput
      """.trimIndent()
  }
}
