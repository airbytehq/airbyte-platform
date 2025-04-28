/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

import io.airbyte.db.factory.DataSourceFactory.create
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import javax.sql.DataSource

/**
 * Temporary factory class that provides convenience methods for creating a [DSLContext]
 * instances. This class will be removed once the project has been converted to leverage an
 * application framework to manage the creation and injection of [DSLContext] objects.
 */
object DSLContextFactory {
  /**
   * Constructs a configured [DSLContext] instance using the provided configuration.
   *
   * @param dataSource The [DataSource] used to connect to the database.
   * @param dialect The SQL dialect to use with objects created from this context.
   * @return The configured [DSLContext].
   */
  @JvmStatic
  fun create(
    dataSource: DataSource,
    dialect: SQLDialect = SQLDialect.POSTGRES,
  ): DSLContext = DSL.using(dataSource, dialect)

  /**
   * Constructs a configured [DSLContext] instance using the provided configuration.
   *
   * @param username The username of the database user.
   * @param password The password of the database user.
   * @param driverClassName The fully qualified name of the JDBC driver class.
   * @param jdbcConnectionString The JDBC connection string.
   * @param dialect The SQL dialect to use with objects created from this context.
   * @return The configured [DSLContext].
   */
  @JvmStatic
  fun create(
    username: String,
    password: String,
    driverClassName: String,
    jdbcConnectionString: String,
    dialect: SQLDialect,
  ): DSLContext = DSL.using(create(username, password, driverClassName, jdbcConnectionString), dialect)
}
