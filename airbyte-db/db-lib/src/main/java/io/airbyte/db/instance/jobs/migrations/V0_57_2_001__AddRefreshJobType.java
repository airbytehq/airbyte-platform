/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_2_001__AddRefreshJobType extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_2_001__AddRefreshJobType.class);

  @Override
  public void migrate(Context context) throws Exception {
    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.alterType("job_config_type").addValue("refresh").execute();
  }

}
