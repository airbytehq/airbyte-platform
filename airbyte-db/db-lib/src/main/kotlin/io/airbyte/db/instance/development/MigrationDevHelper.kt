/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.development

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.db.instance.FlywayDatabaseMigrator
import io.airbyte.db.instance.development.FlywayFormatter.formatMigrationInfoList
import io.airbyte.db.instance.development.FlywayFormatter.formatMigrationResult
import io.micronaut.core.util.StringUtils
import org.flywaydb.core.api.ClassProvider
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.migration.JavaMigration
import org.flywaydb.core.api.resolver.MigrationResolver
import org.flywaydb.core.api.resolver.ResolvedMigration
import org.flywaydb.core.internal.resolver.java.ScanningJavaMigrationResolver
import org.flywaydb.core.internal.scanner.LocationScannerCache
import org.flywaydb.core.internal.scanner.ResourceNameCache
import org.flywaydb.core.internal.scanner.Scanner
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.stream.Collectors

/**
 * Migration helper.
 */
object MigrationDevHelper {
  private val LOGGER: Logger = LoggerFactory.getLogger(MigrationDevHelper::class.java)

  const val AIRBYTE_VERSION_ENV_VAR: String = "AIRBYTE_VERSION"
  const val VERSION_ENV_VAR: String = "VERSION"

  /**
   * This method is used for migration development. Run it to see how your migration changes the
   * database schema.
   */
  @JvmStatic
  @Throws(IOException::class)
  fun runLastMigration(migrator: DevDatabaseMigrator) {
    migrator.createBaseline()

    val preMigrationInfoList = migrator.list()
    LOGGER.info(
      "\n==== Pre Migration Info ====\n" +
        formatMigrationInfoList(
          preMigrationInfoList!!,
        ),
    )
    LOGGER.info(
      """
      
      ==== Pre Migration Schema ====
      ${migrator.dumpSchema()}
      
      """.trimIndent(),
    )

    val migrateResult = migrator.migrate()
    LOGGER.info(
      "\n==== Migration Result ====\n" +
        formatMigrationResult(
          migrateResult!!,
        ),
    )

    val postMigrationInfoList = migrator.list()
    LOGGER.info(
      "\n==== Post Migration Info ====\n" +
        formatMigrationInfoList(
          postMigrationInfoList!!,
        ),
    )
    LOGGER.info(
      """
      
      ==== Post Migration Schema ====
      ${migrator.dumpSchema()}
      
      """.trimIndent(),
    )
  }

  @JvmStatic
  @Throws(IOException::class)
  fun createNextMigrationFile(
    dbIdentifier: String,
    migrator: FlywayDatabaseMigrator,
  ) {
    val description = "New_migration"

    val nextMigrationVersion = getNextMigrationVersion(migrator)
    val versionId = nextMigrationVersion.toString().replace("\\.".toRegex(), "_")

    val template = Resources.read("migration_template.txt")
    val newMigration =
      template
        .replace("<db-name>", dbIdentifier)
        .replace("<version-id>".toRegex(), versionId)
        .replace("<description>".toRegex(), description)
        .trim()

    val fileName = String.format("V%s__%s.java", versionId, description)
    val filePath = String.format("src/main/java/io/airbyte/db/instance/%s/migrations/%s", dbIdentifier, fileName)

    LOGGER.info("\n==== New Migration File ====\n$filePath")

    val file = File(Path.of(filePath).toUri())
    Files.createDirectories(file.toPath().parent)

    try {
      PrintWriter(file, StandardCharsets.UTF_8).use { writer ->
        writer.println(newMigration)
      }
    } catch (e: FileNotFoundException) {
      throw IOException(e)
    }
  }

  @JvmStatic
  fun getSecondToLastMigrationVersion(migrator: FlywayDatabaseMigrator): Optional<MigrationVersion> {
    val migrations = getAllMigrations(migrator)
    if (migrations.isEmpty() || migrations.size == 1) {
      return Optional.empty()
    }
    return Optional.of(migrations[migrations.size - 2].version)
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
      PrintWriter(File(Path.of(schemaDumpFile).toUri()), StandardCharsets.UTF_8).use { writer ->
        writer.println(schema)
        if (printSchema) {
          LOGGER.info("\n==== Schema ====\n$schema")
          LOGGER.info("\n==== Dump File ====\nThe schema has been written to: $schemaDumpFile")
        }
      }
    } catch (e: FileNotFoundException) {
      throw IOException(e)
    }
  }

  /**
   * This method is for migration development and testing purposes. So it is not exposed on the
   * interface. Reference: [Flyway.java](https://github.com/flyway/flyway/blob/master/flyway-core/src/main/java/org/flywaydb/core/Flyway.java#L621).
   */
  private fun getAllMigrations(migrator: FlywayDatabaseMigrator): List<ResolvedMigration> {
    val configuration = migrator.flyway.configuration
    val scanner: ClassProvider<JavaMigration> =
      Scanner(
        JavaMigration::class.java,
        false,
        ResourceNameCache(),
        LocationScannerCache(),
        configuration,
      )
    val resolver = ScanningJavaMigrationResolver(scanner, configuration)
    return resolver
      .resolveMigrations(MigrationResolver.Context(configuration, null, null, null, null))
      .stream() // There may be duplicated migration from the resolver.
      .distinct()
      .collect(Collectors.toList())
  }

  private fun getLastMigrationVersion(migrator: FlywayDatabaseMigrator): Optional<MigrationVersion> {
    val migrations = getAllMigrations(migrator)
    if (migrations.isEmpty()) {
      return Optional.empty()
    }
    return Optional.of(migrations.last().version)
  }

  @JvmStatic
  @get:VisibleForTesting
  val currentAirbyteVersion: AirbyteVersion
    get() {
      val airbyteVersion = System.getenv(AIRBYTE_VERSION_ENV_VAR)
      val version = System.getenv(VERSION_ENV_VAR)
      return if (StringUtils.isNotEmpty(airbyteVersion)) {
        AirbyteVersion(airbyteVersion)
      } else if (StringUtils.isNotEmpty(version)) {
        AirbyteVersion(version)
      } else {
        throw IllegalStateException("Cannot find current Airbyte version from environment.")
      }
    }

  /**
   * Turn a migration version to airbyte version and drop the migration id. E.g. "0.29.10.004" ->
   * "0.29.10".
   */
  @JvmStatic
  @VisibleForTesting
  fun getAirbyteVersion(version: MigrationVersion): AirbyteVersion {
    val splits =
      version.version
        .split("\\.".toRegex())
        .dropLastWhile { it.isEmpty() }
        .toTypedArray()
    return AirbyteVersion(splits[0], splits[1], splits[2])
  }

  /**
   * Extract the major, minor, and patch version and join them with underscore. E.g. "0.29.10-alpha"
   * -> "0_29_10",
   */
  @JvmStatic
  @VisibleForTesting
  fun formatAirbyteVersion(version: AirbyteVersion): String =
    String.format("%s_%s_%s", version.majorVersion, version.minorVersion, version.patchVersion)

  /**
   * Extract the migration id. E.g. "0.29.10.001" -> "001".
   */
  @JvmStatic
  @VisibleForTesting
  fun getMigrationId(version: MigrationVersion): String =
    version.version
      .split("\\.".toRegex())
      .dropLastWhile { it.isEmpty() }
      .toTypedArray()[3]

  private fun getNextMigrationVersion(migrator: FlywayDatabaseMigrator): MigrationVersion {
    val lastMigrationVersion = getLastMigrationVersion(migrator)
    val currentAirbyteVersion = currentAirbyteVersion
    return getNextMigrationVersion(currentAirbyteVersion, lastMigrationVersion)
  }

  @VisibleForTesting
  @JvmStatic
  fun getNextMigrationVersion(
    currentAirbyteVersion: AirbyteVersion,
    lastMigrationVersion: Optional<MigrationVersion>,
  ): MigrationVersion {
    // When there is no migration, use the current airbyte version.
    if (lastMigrationVersion.isEmpty) {
      LOGGER.info("No migration exists. Use the current airbyte version {}", currentAirbyteVersion)
      return MigrationVersion.fromVersion(String.format("%s_001", formatAirbyteVersion(currentAirbyteVersion)))
    }

    // When the current airbyte version is greater, use the airbyte version.
    val migrationVersion = lastMigrationVersion.get()
    val migrationAirbyteVersion = getAirbyteVersion(migrationVersion)
    if (currentAirbyteVersion.versionCompareTo(migrationAirbyteVersion) > 0) {
      LOGGER.info(
        "Use the current airbyte version ({}), since it is greater than the last migration version ({})",
        currentAirbyteVersion,
        migrationAirbyteVersion,
      )
      return MigrationVersion.fromVersion(String.format("%s_001", formatAirbyteVersion(currentAirbyteVersion)))
    }

    // When the last migration version is greater, which usually does not happen, use the migration
    // version.
    LOGGER.info(
      "Use the last migration version ({}), since it is greater than or equal to the current airbyte version ({})",
      migrationAirbyteVersion,
      currentAirbyteVersion,
    )
    val lastMigrationId = getMigrationId(migrationVersion)
    LOGGER.info("lastMigrationId: $lastMigrationId")
    val nextMigrationId = String.format("%03d", lastMigrationId.toInt() + 1)
    LOGGER.info("nextMigrationId: $nextMigrationId")
    return MigrationVersion.fromVersion("${migrationAirbyteVersion.serialize()}_$nextMigrationId")
  }
}
