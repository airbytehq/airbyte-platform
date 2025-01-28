/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class V0_50_5_001__CreateOrganizationTableTest extends AbstractConfigsDatabaseTest {

  private static final String ORGANIZATION = "organization";
  private static final String ID = "id";
  private static final String USER_ID = "user_id";
  private static final String NAME = "name";
  private static final String EMAIL = "email";

  @Test
  void test() throws SQLException, IOException {
    final DSLContext context = getDslContext();
    V0_50_5_001__CreateOrganizationTable.createOrganization(context);

    final UUID organizationId = new UUID(0L, 1L);
    final UUID user_id = new UUID(0L, 1L);

    // assert can insert
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(ORGANIZATION))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(USER_ID),
              DSL.field(EMAIL))
          .values(
              organizationId,
              NAME,
              user_id,
              EMAIL)
          .execute();
    });

    // assert primary key is unique
    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(ORGANIZATION))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(USER_ID),
              DSL.field(EMAIL))
          .values(
              organizationId,
              NAME,
              user_id,
              EMAIL)
          .execute();
    });
    Assertions.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint \"organization_pkey\""));
  }

}
