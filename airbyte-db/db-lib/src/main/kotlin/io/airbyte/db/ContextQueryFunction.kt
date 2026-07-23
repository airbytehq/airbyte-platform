/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db

import org.jooq.DSLContext

/**
 * Query.
 *
 * @param <T> return type of query </T>
 * */
fun interface ContextQueryFunction<T> {
  fun query(context: DSLContext): T
}
