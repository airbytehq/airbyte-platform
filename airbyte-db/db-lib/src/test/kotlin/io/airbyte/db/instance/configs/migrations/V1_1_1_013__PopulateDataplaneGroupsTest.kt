/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.AUTO_DATAPLANE_GROUP
import io.airbyte.commons.EU_DATAPLANE_GROUP
import io.airbyte.commons.US_DATAPLANE_GROUP
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.time.OffsetDateTime
import java.util.UUID

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_013__PopulateDataplaneGroupsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_1_013__PopulateDataplaneGroupsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_012__AddUniquenessConstraintToDataplaneClientCredentials()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    val ctx = dslContext!!
    dropOrganizationIdFKFromWorkspace(ctx)
    dropOrganizationIdFKFromDataplanegroup(ctx)
    dropUpdatedByFKFromDataplanegroup(ctx)
  }

  @Test
  fun testDataplaneGroupsAndDataplanesAreCreated() {
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
        UUID.randomUUID(),
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "EU"),
        UUID.randomUUID(),
      ).execute()
    ctx
      .insertInto<Record, UUID, String, String, Boolean, Boolean, OffsetDateTime, OffsetDateTime, Any, UUID>(
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
        OffsetDateTime.now(),
        DSL.field<Any>(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, AUTO_DATAPLANE_GROUP),
        UUID.randomUUID(),
      ).execute()

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx)

    val dataplaneGroupNames = ctx.select(NAME).from(DATAPLANE_GROUP).fetchSet(NAME)
    Assertions.assertTrue(dataplaneGroupNames.contains(US_DATAPLANE_GROUP))
    Assertions.assertTrue(dataplaneGroupNames.contains(EU_DATAPLANE_GROUP))
    Assertions.assertTrue(dataplaneGroupNames.contains(AUTO_DATAPLANE_GROUP))
  }

  @Test
  fun testDefaultDataplaneCreatedIfNoGeography() {
    val ctx = dslContext!!

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx)

    val dataplaneNames = ctx.select(NAME).from(DATAPLANE_GROUP).fetch(NAME)
    Assertions.assertEquals(1, dataplaneNames.size)
    Assertions.assertEquals(AUTO_DATAPLANE_GROUP, dataplaneNames[0])
  }

  @Test
  fun testNoDuplicateDataplaneGroups() {
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
        UUID.randomUUID(),
        "",
        "",
        true,
        false,
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.dataType, "US"),
        UUID.randomUUID(),
      ).execute()

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx)

    val count = ctx.fetchCount(DATAPLANE_GROUP, NAME.eq("US")).toLong()
    Assertions.assertEquals(1, count)
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
    private val WORKSPACE = DSL.table("workspace")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
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
  }
}
