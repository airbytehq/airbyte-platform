/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add a new Organization role as "ORGANIZATION_MEMBER".
 */
public class V0_50_24_006__AddPermissionTypeOrganizationMember extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_006__AddPermissionTypeOrganizationMember.class);
  private static final String PERMISSION_TYPE_ENUM_NAME = "permission_type";
  private static final String ORGANIZATION_MEMBER = "organization_member";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.alterType(PERMISSION_TYPE_ENUM_NAME).addValue(ORGANIZATION_MEMBER).execute();
  }

}
