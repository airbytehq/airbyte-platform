/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.SSO_CONFIG_TABLE
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

/**
 * Add status column to sso_config table to support DRAFT/ACTIVE workflow for SSO configuration validation.
 */
@Suppress("ktlint:standard:class-naming")
class V1_8_1_006__AddStatusToSsoConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createSsoConfigStatusEnumType(ctx)
    addStatusColumnToSsoConfig(ctx)

    log.info { "Migration finished!" }
  }

  /**
   * SSO Config Status enum.
   */
  enum class SsoConfigStatus(
    private val literal: String,
  ) : EnumType {
    DRAFT("draft"),
    ACTIVE("active"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "sso_config_status"
    }
  }

  companion object {
    private fun createSsoConfigStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(SsoConfigStatus.NAME)
        .asEnum(*SsoConfigStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun addStatusColumnToSsoConfig(ctx: DSLContext) {
      val statusColumn =
        DSL.field(
          "status",
          SQLDataType.VARCHAR
            .asEnumDataType(SsoConfigStatus::class.java)
            .nullable(false)
            .defaultValue(SsoConfigStatus.ACTIVE), // maintain backward compatibility by defaulting to ACTIVE
        )

      ctx
        .alterTable(SSO_CONFIG_TABLE)
        .addColumn(statusColumn)
        .execute()
    }
  }
}
