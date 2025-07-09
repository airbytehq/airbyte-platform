/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.db.factory.DSLContextFactory.create
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DataSourceConnectionProvider
import javax.sql.DataSource

object DataSourceUnwrapper {
  // Micronaut-data wraps the injected data sources with transactional semantics, which don't respect
  // our jooq operations and error out. If we inject an unwrapped one, it will be re-wrapped. So we
  // manually unwrap them.
  fun unwrapDataSource(dataSource: DataSource): DataSource = (dataSource as DelegatingDataSource).targetDataSource

  // For some reason, it won't let us provide an unwrapped dsl context as a bean, so we manually
  // unwrap the data source here as well.
  fun unwrapContext(context: DSLContext): DSLContext {
    val datasource = (context.configuration().connectionProvider() as DataSourceConnectionProvider).dataSource()

    return create(unwrapDataSource(datasource), SQLDialect.POSTGRES)
  }
}
