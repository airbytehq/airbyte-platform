/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_57_4_001__AddRefreshSupportTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_57_4_001__AddRefreshSupportTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_57_4_001__AddRefreshSupport()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testSupportRefreshes() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // This check that we can insert in the actor_definition properly with a support_refreshes value
    // which means that the
    // column is properly created.
    insertAdvWithSupportLevel(ctx)
  }

  companion object {
    private fun insertAdvWithSupportLevel(ctx: DSLContext) {
      ctx
        .insertInto(DSL.table("actor_definition"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("actor_type"),
          DSL.field("support_refreshes"),
        ).values(
          UUID.randomUUID(),
          "name",
          ActorType.source,
          true,
        ).execute()
    }
  }
}
