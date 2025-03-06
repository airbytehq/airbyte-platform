/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.data.services.HealthCheckService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class HealthCheckServiceJooqImpl implements HealthCheckService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckServiceJooqImpl.class);

  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public HealthCheckServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Conduct a health check by attempting to read from the database. This query needs to be fast as
   * this call can be made multiple times a second.
   *
   * @return true if read succeeds, even if the table is empty, and false if any error happens.
   */
  @Override
  public boolean healthCheck() {
    try {
      // The only supported database is Postgres, so we can call SELECT 1 to test connectivity.
      database.query(ctx -> ctx.fetch("SELECT 1")).stream().count();
    } catch (final Exception e) {
      LOGGER.error("Health check error: ", e);
      return false;
    }
    return true;
  }

}
