/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_35_59_002__AddActorDefinitionWorkspaceGrantTable.Companion.createActorDefinitionWorkspaceGrant
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_35_59_002__AddActorDefinitionWorkspaceGrantTableTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)

    val actorDefinitionId = UUID(0L, 1L)
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
        actorDefinitionId,
        "name",
        "repo",
        "1.0.0",
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        JSONB.valueOf("{}"),
      ).execute()

    val workspaceId = UUID(0L, 2L)
    context
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
      ).values(
        workspaceId,
        "default",
        "default",
        true,
      ).execute()

    createActorDefinitionWorkspaceGrant(context)
    assertCanInsertActorDefinitionWorkspaceGrant(context, actorDefinitionId, workspaceId)
    assertActorDefinitionWorkspaceGrantConstraints(context)
  }

  private fun assertCanInsertActorDefinitionWorkspaceGrant(
    context: DSLContext,
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ) {
    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table("actor_definition_workspace_grant"))
        .columns(
          DSL.field("actor_definition_id"),
          DSL.field("workspace_id"),
        ).values(
          actorDefinitionId,
          workspaceId,
        ).execute()
    }
  }

  private fun assertActorDefinitionWorkspaceGrantConstraints(context: DSLContext) {
    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(DSL.table("actor_definition_workspace_grant"))
          .columns(
            DSL.field("actor_definition_id"),
            DSL.field("workspace_id"),
          ).values(
            UUID(0L, 3L),
            UUID(0L, 4L),
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("violates foreign key constraint"))
  }
}
