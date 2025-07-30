/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import io.airbyte.data.services.HealthCheckService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext

@Singleton
class HealthCheckServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : HealthCheckService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Conduct a health check by attempting to read from the database. This query needs to be fast as
     * this call can be made multiple times a second.
     *
     * @return true if read succeeds, even if the table is empty, and false if any error happens.
     */
    override fun healthCheck(): Boolean {
      try {
        // The only supported database is Postgres, so we can call SELECT 1 to test connectivity.
        database.query { ctx: DSLContext -> ctx.fetch("SELECT 1") }.stream().count()
      } catch (e: Exception) {
        log.error(e) { "Health check error: " }
        return false
      }
      return true
    }

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
