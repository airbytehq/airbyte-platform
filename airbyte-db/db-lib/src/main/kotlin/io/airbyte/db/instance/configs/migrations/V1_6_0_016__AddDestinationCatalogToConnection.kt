/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_016__AddDestinationCatalogToConnection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)
    addDestinationCatalogColumn(ctx)
    addActorCatalogTypeColumn(ctx)
  }

  internal enum class ActorCatalogType(
    private val literal: String,
  ) : EnumType {
    SOURCE_CATALOG("source_catalog"),
    DESTINATION_CATALOG("destination_catalog"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = ACTOR_CATALOG_TYPE_ENUM_NAME

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val TABLE = "actor_catalog"
    private const val TYPE_COLUMN_NAME = "catalog_type"
    private const val ACTOR_CATALOG_TYPE_ENUM_NAME = "actor_catalog_type"

    fun addDestinationCatalogColumn(ctx: DSLContext) {
      val connectionTable = DSL.table("connection")
      val actorCatalogTable = DSL.table("actor_catalog")
      val actorCatalogId = DSL.field("id", SQLDataType.UUID)
      val destinationCatalogId = DSL.field("destination_catalog_id", SQLDataType.UUID)

      ctx
        .alterTable(connectionTable)
        .addColumnIfNotExists(destinationCatalogId)
        .execute()

      ctx
        .alterTable(connectionTable)
        .add(
          DSL
            .constraint(
              "connection_destination_catalog_id_fkey",
            ).foreignKey(destinationCatalogId)
            .references(actorCatalogTable, actorCatalogId)
            .onDeleteCascade(),
        ).execute()
    }

    fun addActorCatalogTypeColumn(ctx: DSLContext) {
      // create the enum type
      ctx
        .createTypeIfNotExists(ACTOR_CATALOG_TYPE_ENUM_NAME)
        .asEnum(
          ActorCatalogType.SOURCE_CATALOG.literal,
          ActorCatalogType.DESTINATION_CATALOG.literal,
        ).execute()

      // add the column to the table, with a default value of SOURCE_CATALOG
      val typeColumn =
        DSL.field(
          TYPE_COLUMN_NAME,
          SQLDataType.VARCHAR
            .asEnumDataType(
              ActorCatalogType::class.java,
            ).nullable(false)
            .defaultValue(ActorCatalogType.SOURCE_CATALOG),
        )

      ctx
        .alterTable(TABLE)
        .addColumnIfNotExists(typeColumn)
        .execute()

      // drop default
      ctx
        .alterTable(TABLE)
        .alterColumn(typeColumn)
        .dropDefault()
        .execute()
    }
  }
}
