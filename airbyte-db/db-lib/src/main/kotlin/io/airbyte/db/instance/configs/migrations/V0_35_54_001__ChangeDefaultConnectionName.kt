/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Update connection name migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_54_001__ChangeDefaultConnectionName : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    defaultConnectionName(ctx)
  }

  class Actor(
    val name: String,
  )

  class Connection(
    val name: String,
    val connectionId: UUID,
    val sourceId: UUID,
    val destinationId: UUID,
  )

  companion object {
    private const val NAME = "name"

    fun defaultConnectionName(ctx: DSLContext) {
      log.info { "Updating connection name column" }
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field(NAME, SQLDataType.VARCHAR(256).nullable(false))
      val connections = getConnections<Any>(ctx)

      for (connection in connections) {
        val sourceActor =
          getActor<Any>(
            connection.sourceId,
            ctx,
          )
        val destinationActor =
          getActor<Any>(
            connection.destinationId,
            ctx,
          )
        val connectionName = sourceActor.name + " <> " + destinationActor.name

        ctx
          .update(DSL.table("connection"))
          .set(name, connectionName)
          .where(id.eq(connection.connectionId))
          .execute()
      }
    }

    fun <T> getConnections(ctx: DSLContext): List<Connection> {
      log.info { "Get connections having name default" }
      val name = DSL.field(NAME, SQLDataType.VARCHAR(36).nullable(false))
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val sourceId = DSL.field("source_id", SQLDataType.UUID.nullable(false))
      val destinationId = DSL.field("destination_id", SQLDataType.UUID.nullable(false))

      val connectionName = DSL.field(NAME, SQLDataType.VARCHAR(256).nullable(false))
      val results =
        ctx
          .select(DSL.asterisk())
          .from(DSL.table("connection"))
          .where(connectionName.eq("default"))
          .fetch()

      return results
        .stream()
        .map { record: Record ->
          Connection(
            record.get(name),
            record.get(id),
            record.get(sourceId),
            record.get(destinationId),
          )
        }.collect(Collectors.toList())
    }

    fun <T> getActor(
      actorDefinitionId: UUID,
      ctx: DSLContext,
    ): Actor {
      val name = DSL.field(NAME, SQLDataType.VARCHAR(36).nullable(false))
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))

      val results =
        ctx
          .select(DSL.asterisk())
          .from(DSL.table("actor"))
          .where(id.eq(actorDefinitionId))
          .fetch()

      return results
        .stream()
        .map { record: Record -> Actor(record.get(name)) }
        .toList()[0]
    }
  }
}
