/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.Arrays;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_0_007__AddSubscriptionStatusToOrganizationPaymentConfig extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_007__AddSubscriptionStatusToOrganizationPaymentConfig.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<SubscriptionStatus> subscriptionStatusField =
        DSL.field("subscription_status", SQLDataType.VARCHAR.asEnumDataType(SubscriptionStatus.class)
            .nullable(false)
            .defaultValue(SubscriptionStatus.PRE_SUBSCRIPTION));

    ctx.createType(SubscriptionStatus.NAME)
        .asEnum(Arrays.stream(SubscriptionStatus.values()).map(SubscriptionStatus::getLiteral).toArray(String[]::new))
        .execute();

    ctx.alterTable("organization_payment_config")
        .addColumnIfNotExists(subscriptionStatusField)
        .execute();
  }

  public enum SubscriptionStatus implements EnumType {

    PRE_SUBSCRIPTION("pre_subscription"),
    UNSUBSCRIBED("unsubscribed"),
    SUBSCRIBED("subscribed");

    private final String literal;
    public static final String NAME = "subscription_status";

    SubscriptionStatus(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public @NotNull String getLiteral() {
      return literal;
    }

  }

}
