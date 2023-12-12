/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import io.airbyte.db.factory.DSLContextFactory;
import io.micronaut.transaction.jdbc.DelegatingDataSource;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;

public class DataSourceUnwrapper {

  // Micronaut-data wraps the injected data sources with transactional semantics, which don't respect
  // our jooq operations and error out. If we inject an unwrapped one, it will be re-wrapped. So we
  // manually unwrap them.
  public static DataSource unwrapDataSource(final DataSource dataSource) {
    return ((DelegatingDataSource) dataSource).getTargetDataSource();
  }

  // For some reason, it won't let us provide an unwrapped dsl context as a bean, so we manually
  // unwrap the data source here as well.
  public static DSLContext unwrapContext(final DSLContext context) {
    final var datasource = ((DataSourceConnectionProvider) context.configuration().connectionProvider()).dataSource();

    return DSLContextFactory.create(unwrapDataSource(datasource), SQLDialect.POSTGRES);
  }

}
