/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_35_59_001__AddPublicToActorDefinition.Companion.addPublicColumn
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
internal class V0_35_59_001__AddPublicToActorDefinitionTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!

    // necessary to add actor_definition table
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)

    Assertions.assertFalse(publicColumnExists(context))

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

    addPublicColumn(context)

    Assertions.assertTrue(publicColumnExists(context))
    Assertions.assertTrue(publicDefaultsToFalse(context, id))
  }

  companion object {
    protected fun publicColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq("public")),
          ),
      )

    protected fun publicDefaultsToFalse(
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

      return record!!["public"] == false
    }
  }
}
