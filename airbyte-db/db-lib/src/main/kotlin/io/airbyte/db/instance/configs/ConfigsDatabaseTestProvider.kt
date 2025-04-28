/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs

import io.airbyte.commons.resources.Resources
import io.airbyte.config.ConfigSchema
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.factory.DatabaseCheckFactory.Companion.createConfigsDatabaseInitializer
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.DatabaseMigrator
import io.airbyte.db.instance.test.TestDatabaseProvider
import org.flywaydb.core.Flyway
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Configs db test provider.
 */
class ConfigsDatabaseTestProvider(
  private val dslContext: DSLContext,
  private val flyway: Flyway,
) : TestDatabaseProvider {
  @Throws(IOException::class, DatabaseInitializationException::class)
  override fun create(runMigration: Boolean): Database {
    val initalSchema = Resources.read(DatabaseConstants.CONFIGS_INITIAL_SCHEMA_PATH)
    createConfigsDatabaseInitializer(
      dslContext,
      DatabaseConstants.DEFAULT_CONNECTION_TIMEOUT_MS,
      initalSchema,
    ).initialize()

    val database = Database(dslContext)

    if (runMigration) {
      val migrator: DatabaseMigrator = ConfigsDatabaseMigrator(database, flyway)
      migrator.createBaseline()
      migrator.migrate()
    } else {
      // The configs database is considered ready only if there are some seed records. So we need to create at least one record here.
      val timestamp = OffsetDateTime.now()
      ExceptionWrappingDatabase(database).transaction { ctx: DSLContext ->
        ctx
          .insertInto(DSL.table("airbyte_configs"))
          .set(DSL.field("config_id"), UUID.randomUUID().toString())
          .set(DSL.field("config_type"), ConfigSchema.STATE.name)
          .set(DSL.field("config_blob"), JSONB.valueOf("{}"))
          .set(DSL.field("created_at"), timestamp)
          .set(DSL.field("updated_at"), timestamp)
          .execute()
      }
    }

    return database
  }
}
