/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_050__AddOnboardingStatusToOrganizationTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_050__AddOnboardingStatusToOrganizationTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_049__CreateDataplaneDiagnosticTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
  }

  @Test
  fun `adds a nullable onboarding status column that reads null for existing organizations`() {
    val existingOrganization = UUID.randomUUID()
    ctx.execute("INSERT INTO $ORGANIZATION_TABLE (id, name, email) VALUES (?, 'existing', 'existing@example.com')", existingOrganization)

    V2_1_0_050__AddOnboardingStatusToOrganization.addOnboardingStatusColumn(ctx)

    val column =
      ctx.fetchOne(
        """
        SELECT data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?
        """.trimIndent(),
        ORGANIZATION_TABLE,
        ONBOARDING_STATUS,
      )
    assertNotNull(column)
    assertEquals(
      mapOf("data_type" to "character varying", "is_nullable" to "YES", "column_default" to null),
      column.intoMap(),
    )

    assertNull(ctx.fetchValue("SELECT $ONBOARDING_STATUS FROM $ORGANIZATION_TABLE WHERE id = ?", existingOrganization))
  }

  companion object {
    private const val ONBOARDING_STATUS = "onboarding_status"
  }
}
