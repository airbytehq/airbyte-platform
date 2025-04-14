/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_0_007__AddSubscriptionStatusToOrganizationPaymentConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    val subscriptionStatusField =
      DSL.field(
        "subscription_status",
        SQLDataType.VARCHAR
          .asEnumDataType(SubscriptionStatus::class.java)
          .nullable(false)
          .defaultValue(SubscriptionStatus.PRE_SUBSCRIPTION),
      )

    ctx
      .createType(SubscriptionStatus.NAME)
      .asEnum(*SubscriptionStatus.entries.map { it.literal }.toTypedArray())
      .execute()

    ctx
      .alterTable("organization_payment_config")
      .addColumnIfNotExists(subscriptionStatusField)
      .execute()
  }

  enum class SubscriptionStatus(
    private val literal: String,
  ) : EnumType {
    PRE_SUBSCRIPTION("pre_subscription"),
    UNSUBSCRIBED("unsubscribed"),
    SUBSCRIBED("subscribed"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "subscription_status"
    }
  }
}
