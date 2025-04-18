/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.db.instance.FlywayDatabaseMigrator
import io.airbyte.db.instance.development.FlywayFormatter.formatMigrationInfoList
import io.airbyte.db.instance.development.FlywayFormatter.formatMigrationResult
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.internal.info.MigrationInfoServiceImpl
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException

private val log = KotlinLogging.logger {}
internal const val AIRBYTE_VERSION_ENV_VAR: String = "AIRBYTE_VERSION"
internal const val VERSION_ENV_VAR: String = "VERSION"

/**
 * Migration helper.
 */
object MigrationDevHelper {
  /**
   * This method is used for migration development. Run it to see how your migration changes the
   * database schema.
   */
  @JvmStatic
  @Throws(IOException::class)
  fun runLastMigration(migrator: DevDatabaseMigrator) {
    migrator.createBaseline()

    val preMigrationInfoList = migrator.list()
    log.info {
      "\n==== Pre Migration Info ====\n" +
        formatMigrationInfoList(preMigrationInfoList)
    }
    log.info {
      """
      
      ==== Pre Migration Schema ====
      ${migrator.dumpSchema()}
      
      """.trimIndent()
    }

    val migrateResult = migrator.migrate() ?: throw IllegalStateException("MigrationInfo is null")
    log.info {
      "\n==== Migration Result ====\n" +
        formatMigrationResult(migrateResult)
    }

    val postMigrationInfoList = migrator.list()
    log.info {
      "\n==== Post Migration Info ====\n" +
        formatMigrationInfoList(postMigrationInfoList)
    }
    log.info {
      """
      
      ==== Post Migration Schema ====
      ${migrator.dumpSchema()}
      
      """.trimIndent()
    }
  }

  fun createNextMigrationFile(
    dbIdentifier: String,
    migrator: FlywayDatabaseMigrator,
    description: String,
  ) {
    val nextMigrationVersion = getNextMigrationVersion(migrator)
    val versionId = nextMigrationVersion.toString().replace(".", "_")
    val cleanedDescription = description.replace("[^A-Za-z0-9]".toRegex(), "_")

    val template = Resources.read("migration_template.txt")
    val newMigration =
      template
        .replace("<db-name>", dbIdentifier)
        .replace("<version-id>", versionId)
        .replace("<description>".toRegex(), cleanedDescription)
        .trim()

    val fileName = "V${versionId}__$cleanedDescription.kt"
    val filePath = "src/main/kotlin/io/airbyte/db/instance/$dbIdentifier/migrations/$fileName"

    log.info { "\n==== New Migration File ====\n$filePath" }

    File(filePath).apply { parentFile?.mkdirs() }.writeText(newMigration)
  }

  /**
   * Dump schema to file.
   *
   * @param schema to dump
   * @param schemaDumpFile to dump to
   * @param printSchema should dump schema
   * @throws IOException exception while accessing database
   */
  @JvmStatic
  @Throws(IOException::class)
  fun dumpSchema(
    schema: String,
    schemaDumpFile: String,
    printSchema: Boolean,
  ) {
    try {
      File(schemaDumpFile).writeText(schema)
      if (printSchema) {
        log.info { "\n==== Schema ====\n$schema" }
        log.info { "\n==== Dump File ====\nThe schema has been written to: $schemaDumpFile" }
      }
    } catch (e: FileNotFoundException) {
      throw IOException(e)
    }
  }

  @InternalForTesting
  internal val currentAirbyteVersion: AirbyteVersion
    get() {
      val airbyteVersion = System.getenv(AIRBYTE_VERSION_ENV_VAR)
      val version = System.getenv(VERSION_ENV_VAR)
      return when {
        !airbyteVersion.isNullOrEmpty() -> AirbyteVersion(airbyteVersion)
        !version.isNullOrEmpty() -> AirbyteVersion(version)
        else -> throw IllegalStateException("Cannot find current Airbyte version from environment.")
      }
    }

  private fun getNextMigrationVersion(migrator: FlywayDatabaseMigrator): MigrationVersion {
    val lastMigrationVersion = (migrator.flyway.info() as MigrationInfoServiceImpl).resolved()?.last()?.version
    val currentAirbyteVersion = currentAirbyteVersion
    return getNextMigrationVersion(currentAirbyteVersion, lastMigrationVersion)
  }

  @InternalForTesting
  internal fun getNextMigrationVersion(
    currentAirbyteVersion: AirbyteVersion,
    lastMigrationVersion: MigrationVersion?,
  ): MigrationVersion {
    // When there is no migration, use the current airbyte version.
    if (lastMigrationVersion == null) {
      log.info { "No migration exists. Use the current airbyte version $currentAirbyteVersion" }
      return MigrationVersion.fromVersion(String.format("%s_001", currentAirbyteVersion.format()))
    }

    // When the current airbyte version is greater, use the airbyte version.
    val migrationAirbyteVersion: AirbyteVersion = lastMigrationVersion.airbyteVersion
    if (currentAirbyteVersion.versionCompareTo(migrationAirbyteVersion) > 0) {
      log.info {
        "Use the current airbyte version ($currentAirbyteVersion), since it is greater than the last migration version ($migrationAirbyteVersion)"
      }
      return MigrationVersion.fromVersion("${currentAirbyteVersion.format()}_001")
    }

    // When the last migration version is greater, which usually does not happen, use the migration version.
    log.info {
      "Use the last migration version ($migrationAirbyteVersion), since it is greater than or equal to the current airbyte version ($currentAirbyteVersion)"
    }
    val lastMigrationId = lastMigrationVersion.migrationId
    log.info { "lastMigrationId: $lastMigrationId" }

    val nextMigrationId = String.format("%03d", lastMigrationId.toInt() + 1)
    log.info { "nextMigrationId: $nextMigrationId" }

    return MigrationVersion.fromVersion("${migrationAirbyteVersion.format()}_$nextMigrationId")
  }
}

/**
 * Extract the major, minor, and patch version and join them with underscore. E.g. "0.29.10-alpha" -> "0_29_10",
 */
@InternalForTesting
internal fun AirbyteVersion.format(): String = "${majorVersion}_${minorVersion}_$patchVersion"

/**
 * Extract the migration id. E.g. "0.29.10.001" -> "001".
 */
@InternalForTesting
internal val MigrationVersion.migrationId: String
  get() = version.split(".")[3]

/**
 * Turn a migration version to airbyte version and drop the migration id. E.g. "0.29.10.004" ->
 * "0.29.10".
 */
@InternalForTesting
internal val MigrationVersion.airbyteVersion: AirbyteVersion
  get() = version.split(".").let { AirbyteVersion(it[0], it[1], it[2]) }
