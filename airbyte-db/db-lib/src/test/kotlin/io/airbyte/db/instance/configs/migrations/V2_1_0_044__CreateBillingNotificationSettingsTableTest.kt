/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants.BILLING_NOTIFICATION_SETTINGS_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_044__CreateBillingNotificationSettingsTableTest : AbstractConfigsDatabaseTest() {
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
    DevDatabaseMigrator(configsDbMigrator, V2_1_0_043__AddSemanticSearchEnabledToOrganization().version).createBaseline()
    configsDbMigrator.migrate()
    ctx = dslContext!!
  }

  @Test
  fun `creates the settings table with exact thresholds and a nullable email`() {
    assertColumn("organization_id", "uuid", isNullable = false)
    assertColumn("recipient_email", "character varying", isNullable = true, maxLength = 256)
    assertColumn("sync_credit_threshold", "numeric", isNullable = false, defaultContains = "0")
    assertColumn("invoice_amount_threshold", "numeric", isNullable = false, defaultContains = "0")
    assertColumn("created_at", "timestamp with time zone", isNullable = false, defaultContains = "now()")
    assertColumn("updated_at", "timestamp with time zone", isNullable = false, defaultContains = "now()")
  }

  @Test
  fun `defaults both thresholds to zero and leaves recipient_email null`() {
    val organizationId = insertOrganization()
    ctx.execute("INSERT INTO $BILLING_NOTIFICATION_SETTINGS_TABLE (organization_id) VALUES (?)", organizationId)

    val row = fetchSettings(organizationId)
    assertNull(row.get("recipient_email", String::class.java))
    assertEquals(0, BigDecimal.ZERO.compareTo(row.get("sync_credit_threshold", BigDecimal::class.java)))
    assertEquals(0, BigDecimal.ZERO.compareTo(row.get("invoice_amount_threshold", BigDecimal::class.java)))
    assertNotNull(row.get("created_at"))
    assertNotNull(row.get("updated_at"))
  }

  @Test
  fun `stores fractional thresholds exactly`() {
    val organizationId = insertOrganization()
    insertSettings(organizationId, "billing@example.com", BigDecimal("2.5"), BigDecimal("99.99"))

    val row = fetchSettings(organizationId)
    assertEquals("billing@example.com", row.get("recipient_email", String::class.java))
    assertEquals(0, BigDecimal("2.5").compareTo(row.get("sync_credit_threshold", BigDecimal::class.java)))
    assertEquals(0, BigDecimal("99.99").compareTo(row.get("invoice_amount_threshold", BigDecimal::class.java)))
  }

  @Test
  fun `rejects negative thresholds`() {
    val organizationId = insertOrganization()

    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertSettings(organizationId, null, BigDecimal("-1"), BigDecimal.ZERO)
    }
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertSettings(organizationId, null, BigDecimal.ZERO, BigDecimal("-0.01"))
    }
    assertEquals(0, countSettings(organizationId))
  }

  @Test
  fun `allows one row per organization`() {
    val organizationId = insertOrganization()
    insertSettings(organizationId, null, BigDecimal.ONE, BigDecimal.ONE)

    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertSettings(organizationId, null, BigDecimal.TEN, BigDecimal.TEN)
    }
    assertEquals(1, countSettings(organizationId))
  }

  @Test
  fun `requires an existing organization`() {
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertSettings(UUID.randomUUID(), null, BigDecimal.ONE, BigDecimal.ONE)
    }
  }

  private fun insertOrganization(): UUID {
    val organizationId = UUID.randomUUID()
    ctx.execute(
      "INSERT INTO $ORGANIZATION_TABLE (id, name, email) VALUES (?, 'org', 'org@example.com')",
      organizationId,
    )
    return organizationId
  }

  private fun insertSettings(
    organizationId: UUID,
    recipientEmail: String?,
    syncCreditThreshold: BigDecimal,
    invoiceAmountThreshold: BigDecimal,
  ) {
    ctx.execute(
      """
      INSERT INTO $BILLING_NOTIFICATION_SETTINGS_TABLE (organization_id, recipient_email, sync_credit_threshold, invoice_amount_threshold)
      VALUES (?, ?, ?, ?)
      """.trimIndent(),
      organizationId,
      recipientEmail,
      syncCreditThreshold,
      invoiceAmountThreshold,
    )
  }

  private fun countSettings(organizationId: UUID): Int =
    ctx.fetchValue("SELECT COUNT(*)::int FROM $BILLING_NOTIFICATION_SETTINGS_TABLE WHERE organization_id = ?", organizationId) as Int

  private fun fetchSettings(organizationId: UUID) =
    ctx.fetchOne("SELECT * FROM $BILLING_NOTIFICATION_SETTINGS_TABLE WHERE organization_id = ?", organizationId)
      ?: error("No settings row for organization $organizationId")

  private fun assertColumn(
    columnName: String,
    expectedDataType: String,
    isNullable: Boolean,
    defaultContains: String? = null,
    maxLength: Int? = null,
  ) {
    val column =
      ctx.fetchOne(
        """
        SELECT data_type, is_nullable, column_default, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
        """.trimIndent(),
        BILLING_NOTIFICATION_SETTINGS_TABLE,
        columnName,
      ) ?: error("Column $columnName does not exist on $BILLING_NOTIFICATION_SETTINGS_TABLE")

    assertEquals(expectedDataType, column.get("data_type", String::class.java), "$columnName data type")
    assertEquals(if (isNullable) "YES" else "NO", column.get("is_nullable", String::class.java), "$columnName nullability")
    if (defaultContains != null) {
      val columnDefault = column.get("column_default", String::class.java)
      assertEquals(true, columnDefault?.contains(defaultContains), "$columnName default <$columnDefault> should contain <$defaultContains>")
    }
    if (maxLength != null) {
      assertEquals(maxLength, column.get("character_maximum_length", Int::class.java), "$columnName max length")
    }
  }
}
