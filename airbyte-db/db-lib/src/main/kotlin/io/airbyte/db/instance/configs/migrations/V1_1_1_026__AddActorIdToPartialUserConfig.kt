/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_026__AddActorIdToPartialUserConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addActorIdFieldAndForeignKeyConstraint(ctx)
  }

  companion object {
    const val PARTIAL_USER_CONFIG_TABLE_NAME: String = "partial_user_config"
    const val PARTIAL_USER_CONFIG_ACTOR_ID_FK: String = "partial_user_config_actor_id_fkey"
    const val ID_FIELD: String = "id"
    const val ACTOR_TABLE: String = "actor"
    const val ACTOR_ID_FIELD: String = "actor_id"

    @VisibleForTesting
    fun addActorIdFieldAndForeignKeyConstraint(ctx: DSLContext) {
      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .addColumnIfNotExists(DSL.field(ACTOR_ID_FIELD, SQLDataType.UUID))
        .execute()

      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(
          DSL
            .constraint(PARTIAL_USER_CONFIG_ACTOR_ID_FK)
            .foreignKey(ACTOR_ID_FIELD)
            .references(ACTOR_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()
    }
  }
}
