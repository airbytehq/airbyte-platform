/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db

import org.jooq.DSLContext
import org.jooq.impl.DSL

open class Database(
  private val dslContext: DSLContext,
) {
  fun <T> query(transform: ContextQueryFunction<T>): T = transform.query(dslContext)

  fun <T> transaction(transform: ContextQueryFunction<T>): T = dslContext.transactionResult { cfg -> transform.query(DSL.using(cfg)) }
}
