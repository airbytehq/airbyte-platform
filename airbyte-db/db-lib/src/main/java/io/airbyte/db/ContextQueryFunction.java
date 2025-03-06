/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db;

import java.sql.SQLException;
import org.jooq.DSLContext;

/**
 * Query.
 *
 * @param <T> return type of query
 */
@FunctionalInterface
public interface ContextQueryFunction<T> {

  T query(DSLContext context) throws SQLException;

}
