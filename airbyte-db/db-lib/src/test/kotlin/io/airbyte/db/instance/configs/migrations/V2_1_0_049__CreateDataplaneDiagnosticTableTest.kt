/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants.DATAPLANE_DIAGNOSTIC_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_049__CreateDataplaneDiagnosticTableTest : AbstractConfigsDatabaseTest() {
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
    DevDatabaseMigrator(configsDbMigrator, V2_1_0_048__AddConnectWorkloadType().version).createBaseline()
    configsDbMigrator.migrate()
    ctx = dslContext!!
  }

  @Test
  fun `creates the diagnostic table with nullable error and observation fields`() {
    val expected =
      listOf(
        Column("id", "uuid", isNullable = false),
        Column("dataplane_id", "uuid", isNullable = false),
        Column("organization_id", "uuid", isNullable = false),
        Column("diagnostic_kind", "character varying", isNullable = false),
        Column("target_id", "uuid", isNullable = false),
        Column("status", "character varying", isNullable = false),
        Column("checked_at", "timestamp with time zone", isNullable = false),
        Column("created_at", "timestamp with time zone", isNullable = false),
        Column("updated_at", "timestamp with time zone", isNullable = false),
        Column("error_code", "character varying", isNullable = true),
        Column("error_message", "text", isNullable = true),
        Column("observation", "jsonb", isNullable = true),
      )

    assertEquals(expected, columnsOf(DATAPLANE_DIAGNOSTIC_TABLE))
  }

  @Test
  fun `keeps one diagnostic row per dataplane, kind and target`() {
    val dataplaneId = insertDataplane()
    val targetId = UUID.randomUUID()
    insertDiagnostic(dataplaneId, "SECRET_STORAGE", targetId)

    assertThrows<IntegrityConstraintViolationException> {
      insertDiagnostic(dataplaneId, "SECRET_STORAGE", targetId)
    }

    val otherTargetId = UUID.randomUUID()
    insertDiagnostic(dataplaneId, "SECRET_STORAGE", otherTargetId)
    insertDiagnostic(dataplaneId, "IMAGE_PULL", targetId)

    val expected =
      listOf(
        DiagnosticKey("SECRET_STORAGE", targetId),
        DiagnosticKey("SECRET_STORAGE", otherTargetId),
        DiagnosticKey("IMAGE_PULL", targetId),
      )
    assertEquals(expected.sortedWith(diagnosticKeyOrder), diagnosticKeysOf(dataplaneId))
  }

  @Test
  fun `requires an existing dataplane`() {
    assertThrows<IntegrityConstraintViolationException> {
      insertDiagnostic(UUID.randomUUID(), "SECRET_STORAGE", UUID.randomUUID())
    }
  }

  @Test
  fun `deletes diagnostic rows with their dataplane`() {
    val dataplaneId = insertDataplane()
    val otherDataplaneId = insertDataplane()
    insertDiagnostic(dataplaneId, "SECRET_STORAGE", UUID.randomUUID())
    val otherTargetId = UUID.randomUUID()
    insertDiagnostic(otherDataplaneId, "SECRET_STORAGE", otherTargetId)

    ctx.execute("DELETE FROM dataplane WHERE id = ?", dataplaneId)

    assertEquals(emptyList<DiagnosticKey>(), diagnosticKeysOf(dataplaneId))
    assertEquals(listOf(DiagnosticKey("SECRET_STORAGE", otherTargetId)), diagnosticKeysOf(otherDataplaneId))
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

  private fun insertDataplane(): UUID {
    val organizationId = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO $ORGANIZATION_TABLE (id, name, email) VALUES (?, 'org', 'org@example.com')",
      organizationId,
    )

    val dataplaneGroupId = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO dataplane_group (id, organization_id, name) VALUES (?, ?, 'region')",
      dataplaneGroupId,
      organizationId,
    )

    val dataplaneId = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO dataplane (id, dataplane_group_id, name) VALUES (?, ?, 'dataplane')",
      dataplaneId,
      dataplaneGroupId,
    )
    return dataplaneId
  }

  private fun insertDiagnostic(
    dataplaneId: UUID,
    diagnosticKind: String,
    targetId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO $DATAPLANE_DIAGNOSTIC_TABLE (id, dataplane_id, organization_id, diagnostic_kind, target_id, status, checked_at)
      VALUES (?, ?, ?, ?, ?, 'PASSED', NOW())
      """.trimIndent(),
      UUID.randomUUID(),
      dataplaneId,
      UUID.randomUUID(),
      diagnosticKind,
      targetId,
    )
  }

  private data class DiagnosticKey(
    val diagnosticKind: String,
    val targetId: UUID,
  )

  private val diagnosticKeyOrder = compareBy<DiagnosticKey>({ it.diagnosticKind }, { it.targetId })

  private fun diagnosticKeysOf(dataplaneId: UUID): List<DiagnosticKey> =
    ctx
      .fetch("SELECT diagnostic_kind, target_id FROM $DATAPLANE_DIAGNOSTIC_TABLE WHERE dataplane_id = ?", dataplaneId)
      .map { row ->
        DiagnosticKey(
          diagnosticKind = row.get("diagnostic_kind", String::class.java),
          targetId = row.get("target_id", UUID::class.java),
        )
      }.sortedWith(diagnosticKeyOrder)
}
