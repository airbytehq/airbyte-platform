/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server

import io.airbyte.db.check.DatabaseCheckException
import io.airbyte.db.check.DatabaseMigrationCheck
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import jakarta.inject.Named
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Listen for the db become ready (with migrations run).
 */
@Singleton
class DatabaseEventListener(
  @param:Named("configsDatabaseMigrationCheck") private val configsMigrationCheck: DatabaseMigrationCheck,
  @param:Named("jobsDatabaseMigrationCheck") private val jobsMigrationCheck: DatabaseMigrationCheck,
) : ApplicationEventListener<ServiceReadyEvent?> {
  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    log.info { "Checking configs database flyway migration version..." }
    try {
      configsMigrationCheck.check()
    } catch (e: DatabaseCheckException) {
      throw RuntimeException(e)
    }

    log.info { "Checking jobs database flyway migration version..." }
    try {
      jobsMigrationCheck.check()
    } catch (e: DatabaseCheckException) {
      throw RuntimeException(e)
    }
  }
}
