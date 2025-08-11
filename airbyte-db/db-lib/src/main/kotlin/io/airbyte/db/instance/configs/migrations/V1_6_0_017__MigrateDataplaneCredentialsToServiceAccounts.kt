/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_017__MigrateDataplaneCredentialsToServiceAccounts : BaseJavaMigration() {
  data class DataplaneWithCredentials(
    val id: UUID,
    val name: String,
    val secret: String,
    val clientId: String, // this is going to be a UUID, but the column is varchar
    val createdAt: OffsetDateTime,
  )

  @VisibleForTesting
  enum class PermissionType(
    private val literal: String,
  ) : EnumType {
    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_RUNNER("organization_runner"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_RUNNER("workspace_runner"),
    WORKSPACE_READER("workspace_reader"),
    DATAPLANE("dataplane"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "permission_type"
    }
  }

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    addServiceAccountIdColumnToDataplane(ctx)

    val existingDataplanes = selectExistingDataplanes(ctx)
    log.info { "existing dataplane client ids to be moved: $existingDataplanes" }

    createServiceAccounts(ctx, existingDataplanes)
    createPermissionForServiceAccounts(ctx, existingDataplanes)
    fillDataplaneServiceAccountsColumn(ctx, existingDataplanes)
  }

  companion object {
    fun addServiceAccountIdColumnToDataplane(ctx: DSLContext) {
      ctx
        .alterTable(DSL.table("dataplane"))
        .addColumn(DSL.field("service_account_id", UUID::class.java))
        .execute()

      ctx
        .alterTable(DSL.table("dataplane"))
        .add(
          DSL
            .foreignKey(DSL.field("service_account_id"))
            .references(DSL.table("service_accounts"), DSL.field("id"))
            .onDeleteCascade(),
        ).execute()
    }

    fun selectExistingDataplanes(ctx: DSLContext): List<DataplaneWithCredentials> =
      ctx
        .select()
        .from(DATAPLANE_CLIENT_CREDS_TABLE)
        .join(DATAPLANE_TABLE)
        .on(DATAPLANE_ID_FIELD?.eq(DATAPLANE_CREDS_DATAPLANE_ID_FIELD))
        .fetch()
        .stream()
        .map { r ->
          DataplaneWithCredentials(
            id = r.get("dataplane_id") as UUID,
            name = r.get("name") as String,
            secret = r.get("client_secret") as String,
            clientId = r.get("client_id") as String,
            createdAt = r.get("created_at") as OffsetDateTime,
          )
        }.collect(Collectors.toList())
        .groupBy { it.id }
        .values
        .toList()
        .map { it.maxByOrNull { it.createdAt }!! }

    fun createServiceAccounts(
      ctx: DSLContext,
      existingCreds: List<DataplaneWithCredentials>,
    ) {
      for (dataplane in existingCreds) {
        ctx
          .insertInto(SERVICE_ACCOUNTS_TABLE)
          .columns(
            SERVICE_ACCOUNTS_ID_FIELD,
            SERVICE_ACCOUNTS_NAME_FIELD,
            SERVICE_ACCOUNTS_SECRET_FIELD,
            SERVICE_ACCOUNTS_MANAGED_FIELD,
          ).values(
            UUID.fromString(dataplane.clientId),
            "dataplane-${dataplane.id}",
            dataplane.secret,
            true,
          ).onConflictDoNothing() // if there is somehow already a service account for this client id then we can ignore these credentials
          .execute()
      }
    }

    fun createPermissionForServiceAccounts(
      ctx: DSLContext,
      existingCreds: List<DataplaneWithCredentials>,
    ) {
      for (dataplane in existingCreds) {
        ctx
          .insertInto(PERMISSION_TABLE)
          .columns(
            DSL.field(PERMISSION_ID),
            DSL.field(PERMISSION_SERVICE_ACCOUNT_ID),
            DSL.field(
              PERMISSION_TYPE,
              SQLDataType.VARCHAR.asEnumDataType(
                PermissionType::class.java,
              ),
            ),
          ).values(
            UUID.randomUUID(),
            UUID.fromString(dataplane.clientId),
            PermissionType.DATAPLANE,
          ).execute()
      }
    }

    fun fillDataplaneServiceAccountsColumn(
      ctx: DSLContext,
      existingCreds: List<DataplaneWithCredentials>,
    ) {
      for (dataplane in existingCreds) {
        ctx
          .update(DSL.table("dataplane"))
          .set(
            DSL.field("service_account_id"),
            UUID.fromString(dataplane.clientId),
          ).where(DSL.field("id").eq(dataplane.id))
          .execute()
      }
    }

    private val DATAPLANE_CLIENT_CREDS_TABLE = DSL.table("dataplane_client_credentials")
    private val DATAPLANE_CREDS_DATAPLANE_ID_FIELD = DATAPLANE_CLIENT_CREDS_TABLE.field("dataplane_id", SQLDataType.UUID)

    private val SERVICE_ACCOUNTS_TABLE = DSL.table("service_accounts")
    private val SERVICE_ACCOUNTS_ID_FIELD = SERVICE_ACCOUNTS_TABLE.field("id", SQLDataType.UUID)
    private val SERVICE_ACCOUNTS_NAME_FIELD = SERVICE_ACCOUNTS_TABLE.field("name", SQLDataType.VARCHAR)
    private val SERVICE_ACCOUNTS_SECRET_FIELD = SERVICE_ACCOUNTS_TABLE.field("secret", SQLDataType.VARCHAR)
    private val SERVICE_ACCOUNTS_MANAGED_FIELD = SERVICE_ACCOUNTS_TABLE.field("managed", SQLDataType.BOOLEAN)

    private val DATAPLANE_TABLE = DSL.table("dataplane")
    private val DATAPLANE_ID_FIELD = DATAPLANE_TABLE.field("id", SQLDataType.UUID)

    private val PERMISSION_TABLE = DSL.table("permission")
    private const val PERMISSION_ID = "id"
    private const val PERMISSION_SERVICE_ACCOUNT_ID = "service_account_id"
    private const val PERMISSION_TYPE = "permission_type"
  }
}
