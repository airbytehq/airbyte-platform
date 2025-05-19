/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_35_26_001__PersistDiscoveredCatalogTest : AbstractConfigsDatabaseTest() {
  @Test
  fun test() {
    val context = getDslContext()
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)
    V0_35_26_001__PersistDiscoveredCatalog.migrate(context)
    assertCanInsertData(context)
  }

  private fun assertCanInsertData(ctx: DSLContext) {
    Assertions.assertDoesNotThrow {
      val catalogId = UUID.randomUUID()
      val actorId = UUID.randomUUID()
      val actorDefinitionId = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()

      ctx
        .insertInto(DSL.table("workspace"))
        .columns(
          DSL.field("id"),
          DSL.field(NAME),
          DSL.field("slug"),
          DSL.field("initial_setup_complete"),
        ).values(
          workspaceId,
          "default",
          "default",
          true,
        ).execute()
      ctx
        .insertInto(DSL.table("actor_definition"))
        .columns(
          DSL.field("id"),
          DSL.field(NAME),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("actor_type"),
          DSL.field("spec"),
        ).values(
          actorDefinitionId,
          NAME,
          "repo",
          "1.0.0",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
          JSONB.valueOf("{}"),
        ).execute()
      ctx
        .insertInto(DSL.table("actor"))
        .columns(
          DSL.field("id"),
          DSL.field("workspace_id"),
          DSL.field("actor_definition_id"),
          DSL.field(NAME),
          DSL.field("configuration"),
          DSL.field("actor_type"),
          DSL.field("created_at"),
          DSL.field("updated_at"),
        ).values(
          actorId,
          workspaceId,
          actorDefinitionId,
          "some actor",
          JSONB.valueOf("{}"),
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
          OffsetDateTime.now(),
          OffsetDateTime.now(),
        ).execute()
      ctx
        .insertInto(DSL.table("actor_catalog"))
        .columns(
          DSL.field("id"),
          DSL.field("catalog"),
          DSL.field("catalog_hash"),
          DSL.field("created_at"),
        ).values(
          catalogId,
          JSONB.valueOf("{}"),
          "",
          OffsetDateTime.now(),
        ).execute()
      ctx
        .insertInto(DSL.table("actor_catalog_fetch_event"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_catalog_id"),
          DSL.field("actor_id"),
          DSL.field("config_hash"),
          DSL.field("actor_version"),
        ).values(
          UUID.randomUUID(),
          catalogId,
          actorId,
          "",
          "2.0.1",
        ).execute()
    }
  }

  companion object {
    private const val NAME = "name"
  }
}
