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
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Record4
import org.jooq.Table
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_017__AddOrganizationIdToActorOauthParameterTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_1_017__AddOrganizationIdToActorOauthParameter",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_016__AddDataplaneGroupIdToConnection()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    val ctx = dslContext!!
    dropConstraints(ctx)
    V1_1_1_017__AddOrganizationIdToActorOauthParameter.doMigration(ctx)
  }

  @Test
  fun testCreateOverrideWithOrganizationId() {
    val ctx = dslContext!!

    val id = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val configuration = JSONB.valueOf("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}")
    val actorType = ActorType.source

    ctx
      .insertInto(
        ACTOR_OAUTH_PARAMETER,
        ID_FIELD,
        ACTOR_DEFINITION_ID_FIELD,
        ORGANIZATION_ID_FIELD,
        CONFIGURATION_FIELD,
        ACTOR_TYPE_FIELD,
      ).values(id, actorDefinitionId, organizationId, configuration, actorType)
      .execute()

    val oauthActorParamsMappings =
      ctx
        .select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
        .from(ACTOR_OAUTH_PARAMETER)
        .fetchMap(
          { r: Record4<UUID, UUID, UUID, JSONB> -> r.get(ID_FIELD) },
          { r: Record4<UUID, UUID?, UUID?, JSONB?> ->
            Triple.of(
              r.get(
                ORGANIZATION_ID_FIELD,
              ),
              r.get(WORKSPACE_ID_FIELD),
              r.get(CONFIGURATION_FIELD),
            )
          },
        )

    Assertions.assertEquals(organizationId, oauthActorParamsMappings[id]!!.left)
    Assertions.assertNull(oauthActorParamsMappings[id]!!.middle)
    Assertions.assertEquals(configuration, oauthActorParamsMappings[id]!!.right)
  }

  @Test
  fun testCreateOverrideWithWorkspaceId() {
    val ctx = dslContext!!

    val id = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val actorType = ActorType.source

    ctx
      .insertInto(
        ACTOR_OAUTH_PARAMETER,
        ID_FIELD,
        ACTOR_DEFINITION_ID_FIELD,
        WORKSPACE_ID_FIELD,
        CONFIGURATION_FIELD,
        ACTOR_TYPE_FIELD,
      ).values(id, actorDefinitionId, workspaceId, CONFIGURATION, actorType)
      .execute()

    val oauthActorParamsMappings =
      ctx
        .select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
        .from(ACTOR_OAUTH_PARAMETER)
        .fetchMap(
          { r: Record4<UUID, UUID, UUID, JSONB> -> r.get(ID_FIELD) },
          { r: Record4<UUID, UUID?, UUID?, JSONB?> ->
            Triple.of(
              r.get(
                ORGANIZATION_ID_FIELD,
              ),
              r.get(WORKSPACE_ID_FIELD),
              r.get(CONFIGURATION_FIELD),
            )
          },
        )

    Assertions.assertNull(oauthActorParamsMappings[id]!!.left)
    Assertions.assertEquals(workspaceId, oauthActorParamsMappings[id]!!.middle)
    Assertions.assertEquals(
      CONFIGURATION,
      oauthActorParamsMappings[id]!!
        .right,
    )
  }

  @Test
  fun testCreateOverrideWithOrganizationIdAndWorkspaceId() {
    val ctx = dslContext!!

    val id = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val actorType = ActorType.source

    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      ctx
        .insertInto(
          ACTOR_OAUTH_PARAMETER,
          ID_FIELD,
          ACTOR_DEFINITION_ID_FIELD,
          WORKSPACE_ID_FIELD,
          ORGANIZATION_ID_FIELD,
          CONFIGURATION_FIELD,
          ACTOR_TYPE_FIELD,
        ).values(
          id,
          actorDefinitionId,
          workspaceId,
          organizationId,
          CONFIGURATION,
          actorType,
        ).execute()
    }
  }

  @Test
  fun testCreateOverrideWithoutOrganizationIdOrWorkspaceId() {
    val ctx = dslContext!!

    val id = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val actorType = ActorType.source

    ctx
      .insertInto(
        ACTOR_OAUTH_PARAMETER,
        ID_FIELD,
        ACTOR_DEFINITION_ID_FIELD,
        CONFIGURATION_FIELD,
        ACTOR_TYPE_FIELD,
      ).values(id, actorDefinitionId, CONFIGURATION, actorType)
      .execute()

    val oauthActorParamsMappings =
      ctx
        .select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
        .from(ACTOR_OAUTH_PARAMETER)
        .fetchMap(
          { r: Record4<UUID, UUID, UUID, JSONB> -> r.get(ID_FIELD) },
          { r: Record4<UUID, UUID?, UUID?, JSONB?> ->
            Triple.of(
              r.get(
                ORGANIZATION_ID_FIELD,
              ),
              r.get(WORKSPACE_ID_FIELD),
              r.get(CONFIGURATION_FIELD),
            )
          },
        )

    Assertions.assertNull(oauthActorParamsMappings[id]!!.left)
    Assertions.assertNull(oauthActorParamsMappings[id]!!.middle)
    Assertions.assertEquals(
      CONFIGURATION,
      oauthActorParamsMappings[id]!!
        .right,
    )
  }

  companion object {
    val CONFIGURATION: JSONB = JSONB.valueOf("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}")
    val ACTOR_OAUTH_PARAMETER: Table<Record> = DSL.table("actor_oauth_parameter")
    val ID_FIELD: Field<UUID> = DSL.field("id", SQLDataType.UUID)
    val ACTOR_DEFINITION_ID_FIELD: Field<UUID> = DSL.field("actor_definition_id", SQLDataType.UUID)
    val ORGANIZATION_ID_FIELD: Field<UUID> = DSL.field("organization_id", SQLDataType.UUID)
    val WORKSPACE_ID_FIELD: Field<UUID> = DSL.field("workspace_id", SQLDataType.UUID)
    val CONFIGURATION_FIELD: Field<JSONB> = DSL.field("configuration", SQLDataType.JSONB)
    val ACTOR_TYPE_FIELD: Field<ActorType> =
      DSL.field(
        "actor_type",
        SQLDataType.VARCHAR.asEnumDataType(
          ActorType::class.java,
        ),
      )

    private fun dropConstraints(ctx: DSLContext) {
      ctx
        .alterTable(ACTOR_OAUTH_PARAMETER)
        .dropConstraintIfExists(V1_1_1_017__AddOrganizationIdToActorOauthParameter.ONLY_WORKSPACE_OR_ORG_IS_SET)
        .execute()
    }
  }
}
