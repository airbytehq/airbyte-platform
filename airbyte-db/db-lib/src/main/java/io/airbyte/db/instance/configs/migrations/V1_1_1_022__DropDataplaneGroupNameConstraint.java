/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.sql.Connection;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops the dataplane_group_name_matches_geography constraint from the dataplane_group table.
 */
public class V1_1_1_022__DropDataplaneGroupNameConstraint extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_022__DropDataplaneGroupNameConstraint.class);

  @Override
  public void migrate(Context context) throws Exception {
    LOGGER.info("Running migration to drop 'dataplane_group_name_matches_geography' constraint");

    final Connection connection = context.getConnection();
    final DSLContext ctx = DSL.using(connection);

    final boolean constraintExists = ctx.fetchExists(
        DSL.selectOne()
            .from("information_schema.table_constraints")
            .where(DSL.field("table_name").eq("dataplane_group"))
            .and(DSL.field("constraint_name").eq("dataplane_group_name_matches_geography")));

    if (constraintExists) {
      ctx.alterTable("dataplane_group")
          .dropConstraint("dataplane_group_name_matches_geography")
          .execute();
      LOGGER.info("Dropped constraint: dataplane_group_name_matches_geography");
    } else {
      LOGGER.info("Constraint 'dataplane_group_name_matches_geography' does not exist. Nothing to drop.");
    }
  }

}
