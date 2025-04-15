/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_PAYMENT_CONFIG_TABLE
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
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_4_017__CreateOrganizationPaymentConfigTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createPaymentStatusEnumType(ctx)
    createUsageCategoryOverrideEnumType(ctx)
    createOrganizationPaymentConfigTableAndIndexes(ctx)
  }

  enum class PaymentStatus(
    private val literal: String,
  ) : EnumType {
    UNINITIALIZED("uninitialized"),
    OKAY("okay"),
    GRACE_PERIOD("grace_period"),
    DISABLED("disabled"),
    LOCKED("locked"),
    MANUAL("manual"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "payment_status"
    }
  }

  enum class UsageCategoryOverride(
    private val literal: String,
  ) : EnumType {
    FREE("free"),
    INTERNAL("internal"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "usage_category_override"
    }
  }

  companion object {
    fun createPaymentStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(PaymentStatus.NAME)
        .asEnum(*PaymentStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createUsageCategoryOverrideEnumType(ctx: DSLContext) {
      ctx
        .createType(UsageCategoryOverride.NAME)
        .asEnum(*UsageCategoryOverride.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createOrganizationPaymentConfigTableAndIndexes(ctx: DSLContext) {
      val organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(false))
      val paymentProviderId = DSL.field("payment_provider_id", SQLDataType.VARCHAR(256).nullable(true))
      val paymentStatus =
        DSL.field(
          "payment_status",
          SQLDataType.VARCHAR
            .asEnumDataType(
              PaymentStatus::class.java,
            ).nullable(false)
            .defaultValue(PaymentStatus.UNINITIALIZED),
        )
      val gracePeriodEndAt = DSL.field("grace_period_end_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val usageCategoryOverride =
        DSL.field(
          "usage_category_override",
          SQLDataType.VARCHAR
            .asEnumDataType(
              UsageCategoryOverride::class.java,
            ).nullable(true),
        )
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists(ORGANIZATION_PAYMENT_CONFIG_TABLE)
        .column(organizationId)
        .column(paymentProviderId)
        .column(paymentStatus)
        .column(gracePeriodEndAt)
        .column(usageCategoryOverride)
        .column(createdAt)
        .column(updatedAt)
        .constraints(
          DSL.primaryKey(organizationId),
          DSL.unique(paymentProviderId),
          DSL.foreignKey<UUID>(organizationId).references("organization", "id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("organization_payment_config_payment_provider_id_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, paymentProviderId.name)
        .execute()

      ctx
        .createIndexIfNotExists("organization_payment_config_grace_period_end_at_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, gracePeriodEndAt.name)
        .execute()

      ctx
        .createIndexIfNotExists("organization_payment_config_payment_status_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, paymentStatus.name)
        .execute()
    }
  }
}
