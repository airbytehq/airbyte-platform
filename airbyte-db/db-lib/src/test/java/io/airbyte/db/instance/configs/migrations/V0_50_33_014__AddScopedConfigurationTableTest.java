/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigOriginType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class V0_50_33_014__AddScopedConfigurationTableTest extends AbstractConfigsDatabaseTest {

  private static final String SCOPED_CONFIGURATION = "scoped_configuration";
  private static final String ID = "id";
  private static final String KEY = "key";
  private static final String RESOURCE_TYPE = "resource_type";
  private static final String RESOURCE_ID = "resource_id";
  private static final String SCOPE_TYPE = "scope_type";
  private static final String SCOPE_ID = "scope_id";
  private static final String VALUE = "value";
  private static final String DESCRIPTION = "description";
  private static final String REFERENCE_URL = "reference_url";
  private static final String ORIGIN_TYPE = "origin_type";
  private static final String ORIGIN = "origin";
  private static final String EXPIRES_AT = "expires_at";

  @Test
  void test() throws SQLException, IOException {
    final DSLContext context = getDslContext();

    V0_50_33_014__AddScopedConfigurationTable.createResourceTypeEnum(context);
    V0_50_33_014__AddScopedConfigurationTable.createScopeTypeEnum(context);
    V0_50_33_014__AddScopedConfigurationTable.createOriginTypeEnum(context);
    V0_50_33_014__AddScopedConfigurationTable.createScopedConfigurationTable(context);

    final UUID configId = UUID.randomUUID();
    final String configKey = "connectorVersion";
    final String value = "0.1.0";
    final String description = "Version change for OC";
    final String referenceUrl = "https://github.com/airbytehq/airbyte";
    final ConfigResourceType resourceType = ConfigResourceType.ACTOR_DEFINITION;
    final UUID resourceId = UUID.randomUUID();
    final ConfigScopeType scopeType = ConfigScopeType.WORKSPACE;
    final UUID scopeId = UUID.randomUUID();
    final ConfigOriginType originType = ConfigOriginType.USER;
    final String origin = UUID.randomUUID().toString();
    final OffsetDateTime expiresAt = OffsetDateTime.now();

    // assert can insert
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(SCOPED_CONFIGURATION))
          .columns(
              DSL.field(ID),
              DSL.field(KEY),
              DSL.field(RESOURCE_TYPE),
              DSL.field(RESOURCE_ID),
              DSL.field(SCOPE_TYPE),
              DSL.field(SCOPE_ID),
              DSL.field(VALUE),
              DSL.field(DESCRIPTION),
              DSL.field(REFERENCE_URL),
              DSL.field(ORIGIN_TYPE),
              DSL.field(ORIGIN),
              DSL.field(EXPIRES_AT))
          .values(
              configId,
              configKey,
              resourceType,
              resourceId,
              scopeType,
              scopeId,
              value,
              description,
              referenceUrl,
              originType,
              origin,
              expiresAt)
          .execute();
    });

    final UUID configId2 = UUID.randomUUID();
    final UUID scopeId2 = UUID.randomUUID();
    final String value2 = "0.2.0";

    // insert another one
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(SCOPED_CONFIGURATION))
          .columns(
              DSL.field(ID),
              DSL.field(KEY),
              DSL.field(RESOURCE_TYPE),
              DSL.field(RESOURCE_ID),
              DSL.field(SCOPE_TYPE),
              DSL.field(SCOPE_ID),
              DSL.field(VALUE),
              DSL.field(DESCRIPTION),
              DSL.field(REFERENCE_URL),
              DSL.field(ORIGIN_TYPE),
              DSL.field(ORIGIN))
          .values(
              configId2,
              configKey,
              resourceType,
              resourceId,
              scopeType,
              scopeId2,
              value2,
              description,
              referenceUrl,
              originType,
              origin)
          .execute();
    });

    final UUID configId3 = UUID.randomUUID();

    // assert key + resource_type + resource_id + scope_type + scope_id is unique
    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(SCOPED_CONFIGURATION))
          .columns(
              DSL.field(ID),
              DSL.field(KEY),
              DSL.field(RESOURCE_TYPE),
              DSL.field(RESOURCE_ID),
              DSL.field(SCOPE_TYPE),
              DSL.field(SCOPE_ID),
              DSL.field(VALUE),
              DSL.field(DESCRIPTION),
              DSL.field(REFERENCE_URL),
              DSL.field(ORIGIN_TYPE),
              DSL.field(ORIGIN),
              DSL.field(EXPIRES_AT))
          .values(
              configId3,
              configKey,
              resourceType,
              resourceId,
              scopeType,
              scopeId,
              value2,
              description,
              referenceUrl,
              originType,
              origin,
              expiresAt)
          .execute();
    });
    Assertions.assertTrue(e.getMessage()
        .contains("duplicate key value violates unique constraint \"scoped_configuration_key_resource_type_resource_id_scope_ty_key\""));
  }

}
