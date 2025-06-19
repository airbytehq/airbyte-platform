/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_015__PreparePermissionTableForServiceAccounts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    ctx
      .alterTable(PERMISSION_TABLE)
      .addColumnIfNotExists(SERVICE_ACCOUNT_ID_FIELD, SQLDataType.UUID.nullable(true))
      .execute()

    ctx
      .alterTable(PERMISSION_TABLE)
      .add(
        DSL
          .foreignKey(SERVICE_ACCOUNT_ID_FIELD)
          .references(SERVICE_ACCOUNTS_TABLE, ID)
          .onDeleteCascade(),
      ).execute()

    ctx
      .alterTable(PERMISSION_TABLE)
      .alterColumn(USER_ID_FIELD)
      .dropNotNull()
      .execute()

    // This constraint has some rules about when organization_id or workspace_id
    // can be null, and the logic includes which permission_type (role) is being granted.
    // See V0_64_4_002__AddJobRunnerPermissionTypes for the previous update.
    // As we add more capability to the permission system, these database constraints
    // are a maintenance burden – the validation rules are more easily maintained in Kotlin.
    // So, we're dropping this constraint in favor of validation in the server.
    ctx
      .alterTable(PERMISSION_TABLE)
      .dropConstraintIfExists(CONSTRAINT_NAME)
      .execute()

    ctx
      .createIndexIfNotExists(SERVICE_ACCOUNT_ID_INDEX)
      .on(PERMISSION_TABLE, SERVICE_ACCOUNT_ID_FIELD)
      .execute()
  }

  companion object {
    private const val PERMISSION_TABLE = "permission"
    private const val SERVICE_ACCOUNTS_TABLE = "service_accounts"
    private const val SERVICE_ACCOUNT_ID_FIELD = "service_account_id"
    private const val USER_ID_FIELD = "user_id"

    private const val CONSTRAINT_NAME = "permission_check_organization_id_and_workspace_id"

    private const val ID = "id"

    private const val SERVICE_ACCOUNT_ID_INDEX = "permission_service_account_id_index"
  }
}
