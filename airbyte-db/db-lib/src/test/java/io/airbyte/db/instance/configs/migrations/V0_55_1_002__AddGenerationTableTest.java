/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.configs.migrations.V0_55_1_002__AddGenerationTable.STREAM_GENERATION_TABLE_NAME;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.*;

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

class V0_55_1_002__AddGenerationTableTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_55_1_001__AddRefreshesTable", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_55_1_001__AddRefreshesTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() {
    final DSLContext dslContext = getDslContext();
    final boolean tableExists = generationTableExists(dslContext);

    assertFalse(tableExists);

    V0_55_1_002__AddGenerationTable.createGenerationTable(dslContext);

    final boolean tableExistsPostMigration = generationTableExists(dslContext);

    assertTrue(tableExistsPostMigration);

    final Set<String> index = dslContext.select()
        .from(table("pg_indexes"))
        .where(field("tablename").eq(STREAM_GENERATION_TABLE_NAME))
        .fetch()
        .stream()
        .map(c -> c.getValue("indexdef", String.class))
        .collect(Collectors.toSet());
    assertEquals(3, index.size());
    assertTrue(index.contains("CREATE UNIQUE INDEX stream_generation_pkey ON public.stream_generation USING btree (id)"));
    assertTrue(index.contains(
        "CREATE INDEX stream_generation_connection_id_stream_name_generation_id_idx "
            + "ON public.stream_generation USING btree (connection_id, stream_name, generation_id DESC)"));
    assertTrue(index.contains(
        "CREATE INDEX stream_generation_connection_id_stream_name_stream_namespac_idx ON public.stream_generation "
            + "USING btree (connection_id, stream_name, stream_namespace, generation_id DESC)"));
  }

  private static boolean generationTableExists(final DSLContext dslContext) {
    final int size = dslContext.select()
        .from(table("pg_tables"))
        .where(field("tablename").eq(STREAM_GENERATION_TABLE_NAME))
        .fetch()
        .size();
    return size > 0;
  }

}
