/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Record
import org.jooq.TableField
import org.jooq.impl.DSL
import org.jooq.impl.TableImpl
import java.util.UUID

/**
 * Helpers for interacting with the db.
 */
object PersistenceHelpers {
  /**
   * Helper function to handle null or equal case for the optional strings.
   *
   * We need to have an explicit check for null values because NULL != "str" is NULL, not a boolean.
   *
   * @param field the targeted field
   * @param value the value to check
   * @return The Condition that performs the desired check
   */
  fun isNullOrEquals(
    field: Field<String?>,
    value: String?,
  ): Condition = if (value != null) field.eq(value) else field.isNull()

  /**
   * Helper to delete records from the database.
   *
   * @param table the table to delete from
   * @param keyColumn the column to use as a key
   * @param configId the id of the object to delete, must be from the keyColumn
   * @param ctx the db context to use
   */
  fun <T : Record?> deleteConfig(
    table: TableImpl<T>?,
    keyColumn: TableField<T?, UUID?>,
    configId: UUID?,
    ctx: DSLContext,
  ) {
    val isExistingConfig =
      ctx.fetchExists(
        DSL
          .select()
          .from(table)
          .where(keyColumn.eq(configId)),
      )

    if (isExistingConfig) {
      ctx
        .deleteFrom(table)
        .where(keyColumn.eq(configId))
        .execute()
    }
  }
}
