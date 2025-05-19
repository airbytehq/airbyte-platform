/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_35_59_003__AddCustomToActorDefinition.Companion.addCustomColumn
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_35_59_003__AddCustomToActorDefinitionTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = getDslContext()

    // necessary to add actor_definition table
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)

    Assertions.assertFalse(customColumnExists(context))

    val id = UUID.randomUUID()
    context
      .insertInto(DSL.table("actor_definition"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("docker_repository"),
        DSL.field("docker_image_tag"),
        DSL.field("actor_type"),
        DSL.field("spec"),
      ).values(
        id,
        "name",
        "repo",
        "1.0.0",
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        JSONB.valueOf("{}"),
      ).execute()

    addCustomColumn(context)

    Assertions.assertTrue(customColumnExists(context))
    Assertions.assertTrue(customDefaultsToFalse(context, id))
  }

  companion object {
    protected fun customColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq("custom")),
          ),
      )

    protected fun customDefaultsToFalse(
      ctx: DSLContext,
      id: UUID?,
    ): Boolean {
      val record =
        ctx.fetchOne(
          DSL
            .select()
            .from("actor_definition")
            .where(DSL.field("id").eq(id)),
        )

      return record!!["custom"] == false
    }
  }
}
