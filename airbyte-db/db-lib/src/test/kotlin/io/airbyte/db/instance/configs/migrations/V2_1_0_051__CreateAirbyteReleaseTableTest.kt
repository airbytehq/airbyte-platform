/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants.AIRBYTE_RELEASE_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_051__CreateAirbyteReleaseTableTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    DevDatabaseMigrator(configsDbMigrator, V2_1_0_050__AddOnboardingStatusToOrganization().version).createBaseline()
    configsDbMigrator.migrate()
    ctx = dslContext!!
  }

  @AfterEach
  fun afterEach() {
    // The container is shared across tests, so reset the schema to re-run the migration.
    ctx.dropSchemaIfExists("public").cascade().execute()
    ctx.createSchema("public").execute()
    ctx.setSchema("public").execute()
  }

  @Test
  fun `creates an empty release table with required columns`() {
    val expected =
      listOf(
        Column("id", "uuid", isNullable = false),
        Column("version", "character varying", isNullable = false),
        Column("source_commit", "character varying", isNullable = false),
        Column("published_at", "timestamp with time zone", isNullable = false),
        Column("created_at", "timestamp with time zone", isNullable = false),
      )

    assertEquals(expected, columnsOf(AIRBYTE_RELEASE_TABLE))
    assertEquals(emptyList<String>(), versions())
  }

  @Test
  fun `keeps one row per version`() {
    insertRelease("2.4.0", "abc123")

    assertThrows<IntegrityConstraintViolationException> {
      insertRelease("2.4.0", "def456")
    }

    insertRelease("2.4.1", "def456")

    assertEquals(listOf("2.4.0", "2.4.1"), versions())
  }

  @Test
  fun `requires version, source commit and publication time`() {
    assertThrows<IntegrityConstraintViolationException> {
      ctx.execute(
        "INSERT INTO $AIRBYTE_RELEASE_TABLE (id, source_commit, published_at) VALUES (?, 'abc123', NOW())",
        UUID.randomUUID(),
      )
    }
    assertThrows<IntegrityConstraintViolationException> {
      ctx.execute(
        "INSERT INTO $AIRBYTE_RELEASE_TABLE (id, version, published_at) VALUES (?, '2.4.0', NOW())",
        UUID.randomUUID(),
      )
    }
    assertThrows<IntegrityConstraintViolationException> {
      ctx.execute(
        "INSERT INTO $AIRBYTE_RELEASE_TABLE (id, version, source_commit) VALUES (?, '2.4.0', 'abc123')",
        UUID.randomUUID(),
      )
    }

    assertEquals(emptyList<String>(), versions())
  }

  @Test
  fun `defaults created_at and keeps the given publication time`() {
    val publishedAt = OffsetDateTime.of(2026, 9, 1, 12, 0, 0, 0, ZoneOffset.UTC)
    val beforeInsert = ctx.fetchSingle("SELECT NOW() AS now").get("now", OffsetDateTime::class.java)

    insertRelease("2.4.0", "abc123", publishedAt)

    val row = ctx.fetchOne("SELECT published_at, created_at FROM $AIRBYTE_RELEASE_TABLE WHERE version = '2.4.0'")
    assertNotNull(row)
    assertEquals(publishedAt.toInstant(), row.get("published_at", OffsetDateTime::class.java).toInstant())
    val createdAt = row.get("created_at", OffsetDateTime::class.java)
    assertTrue(!createdAt.isBefore(beforeInsert), "created_at=$createdAt beforeInsert=$beforeInsert")
  }

  private data class Column(
    val name: String,
    val dataType: String,
    val isNullable: Boolean,
  )

  private fun columnsOf(table: String): List<Column> =
    ctx
      .fetch(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ?
        ORDER BY ordinal_position
        """.trimIndent(),
        table,
      ).map { row ->
        Column(
          name = row.get("column_name", String::class.java),
          dataType = row.get("data_type", String::class.java),
          isNullable = row.get("is_nullable", String::class.java) == "YES",
        )
      }

  private fun insertRelease(
    version: String,
    sourceCommit: String,
    publishedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  ) {
    ctx.execute(
      "INSERT INTO $AIRBYTE_RELEASE_TABLE (id, version, source_commit, published_at) VALUES (?, ?, ?, ?::timestamptz)",
      UUID.randomUUID(),
      version,
      sourceCommit,
      publishedAt,
    )
  }

  private fun versions(): List<String> =
    ctx
      .fetch("SELECT version FROM $AIRBYTE_RELEASE_TABLE ORDER BY version")
      .map { row -> row.get("version", String::class.java) }
}
