/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.PRIVATE_LINK_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.DefaultDataType
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_024__AddServiceTypeAndServiceConfigToPrivateLink : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    createServiceTypeEnumType(ctx)
    addServiceTypeColumn(ctx)
    addServiceConfigColumn(ctx)
    backfillExistingRows(ctx)
  }

  enum class ServiceType(
    private val literal: String,
  ) : EnumType {
    ENDPOINT("endpoint"),
    STORAGE("storage"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "private_link_service_type"
    }
  }

  companion object {
    private fun createServiceTypeEnumType(ctx: DSLContext) {
      ctx
        .createType(ServiceType.NAME)
        .asEnum(*ServiceType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun addServiceTypeColumn(ctx: DSLContext) {
      val serviceType =
        DSL.field(
          "service_type",
          SQLDataType.VARCHAR
            .asEnumDataType(ServiceType::class.java)
            .nullable(false)
            .defaultValue(ServiceType.ENDPOINT),
        )
      ctx
        .alterTable(PRIVATE_LINK_TABLE)
        .addColumnIfNotExists(serviceType)
        .execute()
    }

    private fun addServiceConfigColumn(ctx: DSLContext) {
      val serviceConfig =
        DSL.field(
          "service_config",
          DefaultDataType(null, String::class.java, "jsonb")
            .nullable(false)
            .defaultValue(DSL.field("'{}'::jsonb", String::class.java)),
        )
      ctx
        .alterTable(PRIVATE_LINK_TABLE)
        .addColumnIfNotExists(serviceConfig)
        .execute()
    }

    /**
     * All existing rows are endpoint type. Copies service_name and service_region into
     * service_config so readers can stop depending on the legacy flat columns. The
     * version=1 stamp lets later readers tell shape generations apart, which matters
     * during rolling deploys (multiple code versions reading the same row) and
     * database restore (older-shape rows resurfacing after newer code has shipped).
     */
    private fun backfillExistingRows(ctx: DSLContext) {
      ctx
        .execute(
          """
          UPDATE $PRIVATE_LINK_TABLE
          SET service_type = 'endpoint',
              service_config = jsonb_build_object(
                'version', 1,
                'name',    service_name,
                'region',  service_region
              )
          WHERE service_config = '{}'::jsonb
          """.trimIndent(),
        )
    }
  }
}
