/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Record2
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_016__AddDataplaneGroupIdToConnectionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val currentEnv = System.getenv("AIRBYTE_EDITION")
    val flyway =
      create(
        dataSource,
        "V1_1_1_016__AddDataplaneGroupIdToConnectionTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_015__AddAirbyteManagedBooleanToSecretConfigTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    val ctx = dslContext!!
    dropConstraintsFromConnection(ctx)
    dropOrganizationIdFKFromDataplanegroup(ctx)
    dropUpdatedByFKFromDataplanegroup(ctx)
    ctx.deleteFrom(DSL.table("dataplane_group")).execute()
    ctx.deleteFrom(DSL.table("connection")).execute()
    try {
      dropNotNullConstraintFromConnection(ctx)
    } catch (e: Exception) {
      // ignore
    }
    setEnv("AIRBYTE_EDITION", currentEnv)
  }

  @Test
  fun testDataplaneGroupIdIsPopulatedCloud() {
    setEnv("AIRBYTE_EDITION", "CLOUD")

    val ctx = dslContext!!
    val usDataplaneGroupId = UUID.randomUUID()
    val euDataplaneGroupId = UUID.randomUUID()
    val usConnectionId = UUID.randomUUID()
    val euConnectionId = UUID.randomUUID()

    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(usDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "US")
      .execute()
    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "EU")
      .execute()

    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        usConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
      ).execute()
    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        euConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
      ).execute()

    V1_1_1_016__AddDataplaneGroupIdToConnection.doMigration(ctx)

    val connectionDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(CONNECTION)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(usDataplaneGroupId, connectionDataplaneMappings[usConnectionId])
    Assertions.assertEquals(euDataplaneGroupId, connectionDataplaneMappings[euConnectionId])

    val isNullable =
      ctx
        .meta()
        .getTables(CONNECTION.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)
  }

  @Test
  fun testDataplaneGroupIdIsPopulatedNullAirbyteEdition() {
    setEnv("AIRBYTE_EDITION", null)

    val ctx = dslContext!!
    val autoDataplaneGroupId = UUID.randomUUID()
    val usConnectionId = UUID.randomUUID()
    val euConnectionId = UUID.randomUUID()

    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(autoDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "AUTO")
      .execute()

    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        usConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
      ).execute()
    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        euConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
      ).execute()

    V1_1_1_016__AddDataplaneGroupIdToConnection.doMigration(ctx)

    val connectionDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(CONNECTION)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(autoDataplaneGroupId, connectionDataplaneMappings[usConnectionId])
    Assertions.assertEquals(autoDataplaneGroupId, connectionDataplaneMappings[euConnectionId])

    val isNullable =
      ctx
        .meta()
        .getTables(CONNECTION.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)
  }

  @Test
  fun testDataplaneGroupIdIsPopulatedNonCloud() {
    setEnv("AIRBYTE_EDITION", "asdf")

    val ctx = dslContext!!
    val autoDataplaneGroupId = UUID.randomUUID()
    val usConnectionId = UUID.randomUUID()
    val euConnectionId = UUID.randomUUID()

    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(autoDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "AUTO")
      .execute()

    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        usConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
      ).execute()
    ctx
      .insertInto(
        CONNECTION,
        ID,
        NAMESPACE_DEFINITION,
        SOURCE_ID,
        DESTINATION_ID,
        NAME,
        CATALOG,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
      ).values(
        euConnectionId,
        DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.dataType, "source"),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "test",
        JSONB.valueOf("{}"),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
      ).execute()

    V1_1_1_016__AddDataplaneGroupIdToConnection.doMigration(ctx)

    val connectionDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(CONNECTION)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(autoDataplaneGroupId, connectionDataplaneMappings[usConnectionId])
    Assertions.assertEquals(autoDataplaneGroupId, connectionDataplaneMappings[euConnectionId])

    val isNullable =
      ctx
        .meta()
        .getTables(CONNECTION.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)
  }

  companion object {
    // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
    val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val CONNECTION = DSL.table("connection")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
    private val DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID)
    private val GEOGRAPHY = DSL.field("geography", Any::class.java)
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val NAME = DSL.field("name", SQLDataType.VARCHAR)
    private val CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val UPDATED_AT = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID)
    private val SOURCE_ID = DSL.field("source_id", SQLDataType.UUID)
    private val DESTINATION_ID = DSL.field("destination_id", SQLDataType.UUID)
    private val CATALOG = DSL.field("catalog", SQLDataType.JSONB)
    private val NAMESPACE_DEFINITION =
      DSL.field(
        "namespace_definition",
        Any::class.java,
      )
    private const val NAMESPACE_DEFINITION_TYPE = "?::namespace_definition_type"
    private const val GEOGRAPHY_TYPE = "?::geography_type"

    private fun dropConstraintsFromConnection(ctx: DSLContext) {
      ctx
        .alterTable(CONNECTION)
        .dropConstraintIfExists("connection_source_id_fkey")
        .execute()
      ctx
        .alterTable(CONNECTION)
        .dropConstraintIfExists("connection_destination_id_fkey")
        .execute()
    }

    private fun dropOrganizationIdFKFromDataplanegroup(ctx: DSLContext) {
      ctx
        .alterTable(DATAPLANE_GROUP)
        .dropConstraintIfExists("dataplane_group_organization_id_fkey")
        .execute()
    }

    private fun dropUpdatedByFKFromDataplanegroup(ctx: DSLContext) {
      ctx
        .alterTable(DATAPLANE_GROUP)
        .dropConstraintIfExists("dataplane_group_updated_by_fkey")
        .execute()
    }

    private fun dropNotNullConstraintFromConnection(ctx: DSLContext) {
      ctx
        .alterTable(CONNECTION)
        .alterColumn("dataplane_group_id")
        .dropNotNull()
        .execute()
    }
  }

  private fun setEnv(
    key: String,
    value: String?,
  ) {
    val env = System.getenv()
    val cl = env::class.java
    val field = cl.getDeclaredField("m")
    field.isAccessible = true
    val writableEnv = field.get(env) as MutableMap<String, String?>
    if (value != null) {
      writableEnv[key] = value
    } else {
      writableEnv.remove(key)
    }
  }
}
