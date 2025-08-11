/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertNotNull

@Suppress("ktlint:standard:class-naming")
class V1_6_0_017__MigrateDataplaneCredentialsToServiceAccountsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_6_0_017__MigrateDataplaneCredentialsToServiceAccountsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_6_0_016__AddDestinationCatalogToConnection()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    clearCredsTable(dslContext!!)
  }

  @Test
  fun `existing credentials are moved to service accounts table`() {
    val ctx = dslContext!!

    val dataplaneId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val orgId = UUID.randomUUID()

    // create an org so we don't collide with the default org
    ctx
      .insertInto(DSL.table("organization"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("email"),
      ).values(
        orgId,
        "org-name",
        "test@email.com",
      ).execute()

    // create a dataplane group
    ctx
      .insertInto(DSL.table("dataplane_group"))
      .columns(
        DSL.field("id"),
        DSL.field("organization_id"),
        DSL.field("name"),
        DSL.field("enabled"),
        DSL.field("tombstone"),
      ).values(
        dataplaneGroupId,
        orgId,
        "test-dataplane-group",
        true,
        false,
      ).execute()

    // create a dataplane for the dataplane group
    ctx
      .insertInto(DSL.table("dataplane"))
      .columns(
        DSL.field("id"),
        DSL.field("dataplane_group_id"),
        DSL.field("name"),
        DSL.field("enabled"),
        DSL.field("tombstone"),
      ).values(
        dataplaneId,
        dataplaneGroupId,
        "dataplane-1",
        true,
        false,
      ).execute()

    val clientId1 = UUID.randomUUID()
    val clientId2 = UUID.randomUUID()

    // create credentials for the dataplane
    ctx
      .insertInto(DSL.table("dataplane_client_credentials"))
      .columns(
        DSL.field("id"),
        DSL.field("dataplane_id"),
        DSL.field("client_id"),
        DSL.field("client_secret"),
      ).values(
        UUID.randomUUID(),
        dataplaneId,
        clientId1,
        "client-secret-1",
      ).execute()

    // create a second set to ensure only one set is selected
    ctx
      .insertInto(DSL.table("dataplane_client_credentials"))
      .columns(
        DSL.field("id"),
        DSL.field("dataplane_id"),
        DSL.field("client_id"),
        DSL.field("client_secret"),
      ).values(
        UUID.randomUUID(),
        dataplaneId,
        clientId2,
        "client-secret-2",
      ).execute()

    V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.addServiceAccountIdColumnToDataplane(ctx)

    // test that the existing credentials can be selected properly
    val creds = V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.selectExistingDataplanes(ctx)
    assertNotNull(creds)
    // this should be select distinct, which should only be one result for this id
    assertEquals(1, creds.size)

    // now actually migrate the existing credentials to the service accounts table
    V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.createServiceAccounts(ctx, creds)

    val result =
      ctx
        .selectFrom(DSL.table("service_accounts"))
        .where(DSL.field("id").eq(clientId2))
        .fetchOne()

    assertNotNull(result)
    // The second client ID is used, because it is the most recent.
    assertEquals(clientId2, result.get("id"))
    assertEquals("client-secret-2", result.get("secret"))
    assertEquals("dataplane-$dataplaneId", result.get("name"))

    // create the dataplane permission for the service account
    V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.createPermissionForServiceAccounts(ctx, creds)

    val permission =
      ctx
        .select(
          DSL.field("service_account_id"),
          DSL.field(
            "permission_type",
            SQLDataType.VARCHAR.asEnumDataType(V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.PermissionType::class.java),
          ),
        ).from(DSL.table("permission"))
        .where(DSL.field("service_account_id").eq(clientId2))
        .fetchOne()

    assertNotNull(permission)
    assertEquals(clientId2, permission.get("service_account_id"))
    assertEquals(V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.PermissionType.DATAPLANE, permission.get("permission_type"))

    V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts.fillDataplaneServiceAccountsColumn(ctx, creds)

    val dataplane =
      ctx
        .selectFrom(DSL.table("dataplane"))
        .where(DSL.field("id").eq(dataplaneId))
        .fetchOne()

    assertNotNull(dataplane)
    assertEquals(clientId2, dataplane.get("service_account_id"))
  }

  private fun clearCredsTable(ctx: DSLContext) {
    ctx.truncate(DSL.table("dataplane_client_credentials")).execute()
  }
}
