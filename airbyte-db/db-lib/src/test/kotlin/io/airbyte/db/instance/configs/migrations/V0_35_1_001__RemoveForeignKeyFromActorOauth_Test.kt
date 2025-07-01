/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException

@Suppress("ktlint:standard:class-naming")
internal class V0_35_1_001__RemoveForeignKeyFromActorOauth_Test : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(IOException::class, SQLException::class)
  fun testCompleteMigration() {
    val context = dslContext!!
    SetupForNormalizedTablesTest.setup(context)

    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)
    V0_35_1_001__RemoveForeignKeyFromActorOauth.migrate(context)
    assertDataForSourceOauthParams(context)
    assertDataForDestinationOauthParams(context)
  }

  private fun assertDataForSourceOauthParams(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val sourceOauthParams =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_oauth_parameter"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.sourceOauthParameters()
    Assertions.assertEquals(expectedDefinitions.size, sourceOauthParams.size)

    for (record in sourceOauthParams) {
      val sourceOAuthParameter =
        SourceOAuthParameter()
          .withOauthParameterId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withSourceDefinitionId(record.get(actorDefinitionId))
      Assertions.assertTrue(expectedDefinitions.contains(sourceOAuthParameter))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }
  }

  private fun assertDataForDestinationOauthParams(context: DSLContext) {
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
    val actorType =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java,
          ).nullable(false),
      )
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

    val destinationOauthParams =
      context
        .select(DSL.asterisk())
        .from(DSL.table("actor_oauth_parameter"))
        .where(actorType.eq(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.destination))
        .fetch()
    val expectedDefinitions = SetupForNormalizedTablesTest.destinationOauthParameters()
    Assertions.assertEquals(expectedDefinitions.size, destinationOauthParams.size)

    for (record in destinationOauthParams) {
      val destinationOAuthParameter =
        DestinationOAuthParameter()
          .withOauthParameterId(record.get(id))
          .withConfiguration(Jsons.deserialize(record.get(configuration).data()))
          .withWorkspaceId(record.get(workspaceId))
          .withDestinationDefinitionId(record.get(actorDefinitionId))
      Assertions.assertTrue(expectedDefinitions.contains(destinationOAuthParameter))
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(createdAt).toInstant())
      Assertions.assertEquals(SetupForNormalizedTablesTest.now(), record.get(updatedAt).toInstant())
    }
  }
}
