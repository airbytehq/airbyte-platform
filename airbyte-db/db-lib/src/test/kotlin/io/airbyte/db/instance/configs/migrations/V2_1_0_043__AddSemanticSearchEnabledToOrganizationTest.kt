/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.jooq.exception.IntegrityConstraintViolationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_043__AddSemanticSearchEnabledToOrganizationTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `defaults existing and new organizations to disabled and enforces non-null enablement`() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        javaClass.simpleName,
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    DevDatabaseMigrator(ConfigsDatabaseMigrator(database!!, flyway), V2_1_0_042__AddConnectionEnablements().version).createBaseline()
    val ctx = dslContext!!
    val existingOrganization = UUID.randomUUID()
    ctx.execute("INSERT INTO $ORGANIZATION_TABLE (id, name, email) VALUES (?, 'existing', 'existing@example.com')", existingOrganization)

    ConfigsDatabaseMigrator(database!!, flyway).migrate()

    assertEquals(false, ctx.fetchValue("SELECT semantic_search_enabled FROM $ORGANIZATION_TABLE WHERE id = ?", existingOrganization))

    val newOrganization = UUID.randomUUID()
    ctx.execute("INSERT INTO $ORGANIZATION_TABLE (id, name, email) VALUES (?, 'new', 'new@example.com')", newOrganization)
    assertEquals(false, ctx.fetchValue("SELECT semantic_search_enabled FROM $ORGANIZATION_TABLE WHERE id = ?", newOrganization))

    ctx.execute("UPDATE $ORGANIZATION_TABLE SET semantic_search_enabled = true WHERE id = ?", newOrganization)
    assertEquals(true, ctx.fetchValue("SELECT semantic_search_enabled FROM $ORGANIZATION_TABLE WHERE id = ?", newOrganization))
    assertEquals(false, ctx.fetchValue("SELECT semantic_search_enabled FROM $ORGANIZATION_TABLE WHERE id = ?", existingOrganization))

    ctx.execute("UPDATE $ORGANIZATION_TABLE SET semantic_search_enabled = false WHERE id = ?", newOrganization)
    assertEquals(false, ctx.fetchValue("SELECT semantic_search_enabled FROM $ORGANIZATION_TABLE WHERE id = ?", newOrganization))

    assertThrows(IntegrityConstraintViolationException::class.java) {
      ctx.execute("UPDATE $ORGANIZATION_TABLE SET semantic_search_enabled = NULL WHERE id = ?", newOrganization)
    }
    assertThrows(IntegrityConstraintViolationException::class.java) {
      ctx.execute(
        "INSERT INTO $ORGANIZATION_TABLE (id, name, email, semantic_search_enabled) VALUES (?, 'null', 'null@example.com', NULL)",
        UUID.randomUUID(),
      )
    }
  }
}
