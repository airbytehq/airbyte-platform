/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_PAYMENT_CONFIG_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;
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

public class V0_57_4_017__CreateOrganizationPaymentConfigTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_017__CreateOrganizationPaymentConfigTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    createPaymentStatusEnumType(ctx);
    createUsageCategoryOverrideEnumType(ctx);
    createOrganizationPaymentConfigTableAndIndexes(ctx);
  }

  static void createPaymentStatusEnumType(final DSLContext ctx) {
    ctx.createType(PaymentStatus.NAME)
        .asEnum(Arrays.stream(PaymentStatus.values()).map(PaymentStatus::getLiteral).toArray(String[]::new))
        .execute();
  }

  static void createUsageCategoryOverrideEnumType(final DSLContext ctx) {
    ctx.createType(UsageCategoryOverride.NAME)
        .asEnum(Arrays.stream(UsageCategoryOverride.values()).map(UsageCategoryOverride::getLiteral).toArray(String[]::new))
        .execute();
  }

  static void createOrganizationPaymentConfigTableAndIndexes(final DSLContext ctx) {
    final Field<UUID> organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(false));
    final Field<String> paymentProviderId = DSL.field("payment_provider_id", SQLDataType.VARCHAR(256).nullable(true));
    final Field<PaymentStatus> paymentStatus = DSL.field("payment_status",
        SQLDataType.VARCHAR.asEnumDataType(PaymentStatus.class).nullable(false).defaultValue(PaymentStatus.UNINITIALIZED));
    final Field<OffsetDateTime> gracePeriodEndAt = DSL.field("grace_period_end_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true));
    final Field<UsageCategoryOverride> usageCategoryOverride =
        DSL.field("usage_category_override", SQLDataType.VARCHAR.asEnumDataType(UsageCategoryOverride.class).nullable(true));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(ORGANIZATION_PAYMENT_CONFIG_TABLE)
        .column(organizationId)
        .column(paymentProviderId)
        .column(paymentStatus)
        .column(gracePeriodEndAt)
        .column(usageCategoryOverride)
        .column(createdAt)
        .column(updatedAt)
        .constraints(
            primaryKey(organizationId),
            unique(paymentProviderId),
            foreignKey(organizationId).references("organization", "id").onDeleteCascade())
        .execute();

    ctx.createIndexIfNotExists("organization_payment_config_payment_provider_id_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, paymentProviderId.getName())
        .execute();

    ctx.createIndexIfNotExists("organization_payment_config_grace_period_end_at_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, gracePeriodEndAt.getName())
        .execute();

    ctx.createIndexIfNotExists("organization_payment_config_payment_status_idx")
        .on(ORGANIZATION_PAYMENT_CONFIG_TABLE, paymentStatus.getName())
        .execute();
  }

  public enum PaymentStatus implements EnumType {

    UNINITIALIZED("uninitialized"),
    OKAY("okay"),
    GRACE_PERIOD("grace_period"),
    DISABLED("disabled"),
    LOCKED("locked"),
    MANUAL("manual");

    private final String literal;
    public static final String NAME = "payment_status";

    PaymentStatus(final String literal) {
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

  public enum UsageCategoryOverride implements EnumType {

    FREE("free"),
    INTERNAL("internal");

    private final String literal;
    public static final String NAME = "usage_category_override";

    UsageCategoryOverride(final String literal) {
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
