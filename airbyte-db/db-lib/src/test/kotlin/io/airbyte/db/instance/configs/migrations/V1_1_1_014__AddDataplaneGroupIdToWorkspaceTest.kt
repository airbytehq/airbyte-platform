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
import org.jooq.Record2
import org.jooq.Table
import org.jooq.exception.DataAccessException
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.time.OffsetDateTime
import java.util.UUID

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_014__AddDataplaneGroupIdToWorkspaceTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val currentEnv = System.getenv("AIRBYTE_EDITION")

    val flyway =
      create(
        dataSource,
        "V1_1_1_014__AddDataplaneGroupIdToWorkspaceTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_013__PopulateDataplaneGroups()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    val ctx = dslContext!!
    dropOrganizationIdFKFromWorkspace(ctx)
    dropOrganizationIdFKFromDataplanegroup(ctx)
    dropUpdatedByFKFromDataplanegroup(ctx)
    try {
      dropNotNullConstraintFromWorkspace(ctx)
    } catch (e: Exception) {
      // ignore
    }
    ctx.deleteFrom(DSL.table("dataplane_group")).execute()
    ctx.deleteFrom(DSL.table("workspace")).execute()
    recreateColumnGeography(ctx)
    dropColumnGeographyDONOTUSE(ctx)
    setEnv("AIRBYTE_EDITION", currentEnv)
  }

  @Test
  @Order(10)
  fun testDataplaneGroupIdIsPopulatedCloud() {
    setEnv("AIRBYTE_EDITION", "CLOUD")

    val ctx = dslContext!!
    val usDataplaneGroupId = UUID.randomUUID()
    val euDataplaneGroupId = UUID.randomUUID()
    val usWorkspaceId = UUID.randomUUID()
    val euWorkspaceId = UUID.randomUUID()
    val autoWorkspaceId = UUID.randomUUID()

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
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        usWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
        UUID.randomUUID(),
      ).execute()
    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        euWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
        UUID.randomUUID(),
      ).execute()

    V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx)

    val workspaceDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(WORKSPACE)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings[usWorkspaceId])
    Assertions.assertEquals(euDataplaneGroupId, workspaceDataplaneMappings[euWorkspaceId])

    val isNullable =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)

    val columnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography" == field.name }
    Assertions.assertFalse(columnExists)

    val deprecatedColumnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography_DO_NOT_USE" == field.name }
    Assertions.assertTrue(deprecatedColumnExists)
  }

  @Test
  @Order(11)
  fun testDataplaneGroupIdIsPopulatedNonCloud() {
    setEnv("AIRBYTE_EDITION", "asdf")

    val ctx = dslContext!!
    val usDataplaneGroupId = UUID.randomUUID()
    val usWorkspaceId = UUID.randomUUID()
    val euWorkspaceId = UUID.randomUUID()
    val autoWorkspaceId = UUID.randomUUID()

    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(usDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "US")
      .execute()

    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        usWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
        UUID.randomUUID(),
      ).execute()
    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        euWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
        UUID.randomUUID(),
      ).execute()

    V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx)

    val workspaceDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(WORKSPACE)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings[usWorkspaceId])
    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings[euWorkspaceId])

    // AUTO is the default dataplane group and should always exist
    val autoDataplaneGroupId =
      ctx
        .select(ID)
        .from(DATAPLANE_GROUP)
        .where(NAME.eq("AUTO"))
        .fetchOneInto(
          UUID::class.java,
        )
    Assertions.assertEquals(autoDataplaneGroupId, workspaceDataplaneMappings[autoWorkspaceId])

    val isNullable =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)

    val columnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography" == field.name }
    Assertions.assertFalse(columnExists)

    val deprecatedColumnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography_DO_NOT_USE" == field.name }
    Assertions.assertTrue(deprecatedColumnExists)
  }

  @Test
  @Order(12)
  fun testDataplaneGroupIdIsPopulatedNullAirbyteEdition() {
    setEnv("AIRBYTE_EDITION", null)

    val ctx = dslContext!!
    val usDataplaneGroupId = UUID.randomUUID()
    val euDataplaneGroupId = UUID.randomUUID()
    val usWorkspaceId = UUID.randomUUID()
    val euWorkspaceId = UUID.randomUUID()
    val autoWorkspaceId = UUID.randomUUID()

    ctx
      .insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
      .values(usDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "US")
      .execute()

    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        usWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
        UUID.randomUUID(),
      ).execute()
    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        euWorkspaceId,
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
        UUID.randomUUID(),
      ).execute()

    V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx)

    val workspaceDataplaneMappings =
      ctx
        .select(ID, DATAPLANE_GROUP_ID)
        .from(WORKSPACE)
        .fetchMap(
          { r: Record2<UUID, UUID> -> r.get(ID) },
          { r: Record2<UUID, UUID?> -> r.get(DATAPLANE_GROUP_ID) },
        )

    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings[usWorkspaceId])
    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings[euWorkspaceId])

    // AUTO is the default dataplane group and should always exist
    val autoDataplaneGroupId =
      ctx
        .select(ID)
        .from(DATAPLANE_GROUP)
        .where(NAME.eq("AUTO"))
        .fetchOneInto(
          UUID::class.java,
        )
    Assertions.assertEquals(autoDataplaneGroupId, workspaceDataplaneMappings[autoWorkspaceId])

    val isNullable =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .filter { field: Field<*> -> "dataplane_group_id" == field.name }
        .anyMatch { field: Field<*> -> field.dataType.nullable() }
    Assertions.assertFalse(isNullable)

    val columnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography" == field.name }
    Assertions.assertFalse(columnExists)

    val deprecatedColumnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography_DO_NOT_USE" == field.name }
    Assertions.assertTrue(deprecatedColumnExists)
  }

  @Test
  @Order(1)
  fun testMigrationThrowsOnCloudWhenDataplaneGroupIdNotFoundForGeography() {
    setEnv("AIRBYTE_EDITION", "CLOUD")

    val ctx = dslContext!!
    ctx
      .insertInto(
        WORKSPACE,
        ID,
        NAME,
        SLUG,
        INITIAL_SETUP_COMPLETE,
        TOMBSTONE,
        CREATED_AT,
        UPDATED_AT,
        GEOGRAPHY,
        ORGANIZATION_ID,
      ).values(
        UUID.randomUUID(),
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(), // At this point only the dataplane group named "AUTO" exists.
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
        UUID.randomUUID(),
      ).execute()

    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx)
    }

    val columnExists =
      ctx
        .meta()
        .getTables(WORKSPACE.name)
        .stream()
        .flatMap { obj: Table<*> -> obj.fieldStream() }
        .anyMatch { field: Field<*> -> "geography" == field.name }
    Assertions.assertTrue(columnExists)
  }

  @Test
  fun testNameConstraintThrowsOnNotAllowedValues() {
    val ctx = dslContext!!
    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      ctx
        .insertInto(
          DATAPLANE_GROUP,
          ID,
          NAME,
        ).values(UUID.randomUUID(), "INVALID")
        .execute()
    }
  }

  companion object {
    // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
    val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val WORKSPACE = DSL.table("workspace")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
    private val DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID)
    private val GEOGRAPHY = DSL.field("geography", Any::class.java)
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val NAME = DSL.field("name", SQLDataType.VARCHAR)
    private val SLUG = DSL.field("slug", SQLDataType.VARCHAR)
    private val INITIAL_SETUP_COMPLETE = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN)
    private val TOMBSTONE = DSL.field("tombstone", SQLDataType.BOOLEAN)
    private val CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val UPDATED_AT = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID)
    private const val GEOGRAPHY_TYPE = "?::geography_type"

    private fun dropOrganizationIdFKFromWorkspace(ctx: DSLContext) {
      ctx
        .alterTable(WORKSPACE)
        .dropConstraintIfExists("workspace_organization_id_fkey")
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

    private fun dropNotNullConstraintFromWorkspace(ctx: DSLContext) {
      ctx
        .alterTable(WORKSPACE)
        .alterColumn("dataplane_group_id")
        .dropNotNull()
        .execute()
    }

    private fun recreateColumnGeography(ctx: DSLContext) {
      try {
        ctx
          .alterTable(WORKSPACE)
          .addColumn("geography", SQLDataType.VARCHAR)
          .execute()
      } catch (e: Exception) {
        // ignore
      }
    }

    private fun dropColumnGeographyDONOTUSE(ctx: DSLContext) {
      try {
        ctx
          .alterTable(WORKSPACE)
          .dropColumn("geography_DO_NOT_USE")
          .execute()
      } catch (e: Exception) {
        // ignore
      }
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
