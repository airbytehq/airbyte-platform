/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import org.jooq.DSLContext
import org.jooq.Query

class StateUpdateBatch {
  val updatedStreamStates: MutableList<Query> = mutableListOf()
  val createdStreamStates: MutableList<Query> = mutableListOf()
  val deletedStreamStates: MutableList<Query> = mutableListOf()

  fun save(ctx: DSLContext) {
    ctx.batch(updatedStreamStates).execute()
    ctx.batch(createdStreamStates).execute()
    ctx.batch(deletedStreamStates).execute()
  }
}
