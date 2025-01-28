/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Query;

class StateUpdateBatch {

  private final List<Query> updatedStreamStates = new ArrayList<>();
  private final List<Query> createdStreamStates = new ArrayList<>();
  private final List<Query> deletedStreamStates = new ArrayList<>();

  void save(final DSLContext ctx) {
    ctx.batch(updatedStreamStates).execute();
    ctx.batch(createdStreamStates).execute();
    ctx.batch(deletedStreamStates).execute();
  }

  public List<Query> getDeletedStreamStates() {
    return deletedStreamStates;
  }

  public List<Query> getCreatedStreamStates() {
    return createdStreamStates;
  }

  public List<Query> getUpdatedStreamStates() {
    return updatedStreamStates;
  }

}
