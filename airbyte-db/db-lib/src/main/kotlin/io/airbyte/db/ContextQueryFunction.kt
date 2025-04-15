/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db

import org.jooq.DSLContext
import java.sql.SQLException

/**
 * Query.
 *
 * @param <T> return type of query </T>
 * */
fun interface ContextQueryFunction<T> {
  @Throws(SQLException::class)
  fun query(context: DSLContext): T
}
