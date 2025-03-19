/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.configs.migrations.V0_55_1_003__EditRefreshTable.STREAM_REFRESHES_TABLE;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.Set;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_55_1_003__EditRefreshTableTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_55_1_002__AddGenerationTable", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_55_1_002__AddGenerationTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() {
    final DSLContext dslContext = getDslContext();
    V0_55_1_003__EditRefreshTable.editRefreshTable(dslContext);
    final Set<String> index = dslContext.select()
        .from(table("pg_indexes"))
        .where(field("tablename").eq(STREAM_REFRESHES_TABLE))
        .fetch()
        .stream()
        .map(c -> c.getValue("indexdef", String.class))
        .collect(Collectors.toSet());
    assertEquals(4, index.size());
    assertTrue(index.contains(
        "CREATE UNIQUE INDEX stream_refreshes_pkey ON public.stream_refreshes USING btree (id)"));
    assertTrue(index.contains(
        "CREATE INDEX stream_refreshes_connection_id_idx ON public.stream_refreshes USING btree (connection_id)"));
    assertTrue(index.contains(
        "CREATE INDEX stream_refreshes_connection_id_stream_name_idx ON public.stream_refreshes "
            + "USING btree (connection_id, stream_name)"));
    assertTrue(index.contains(
        "CREATE INDEX stream_refreshes_connection_id_stream_name_stream_namespace_idx ON public.stream_refreshes"
            + " USING btree (connection_id, stream_name, stream_namespace)"));
  }

}
