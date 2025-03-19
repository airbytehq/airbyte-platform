/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

class V0_50_41_004__AddDeadlineColumnToWorkloadTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_41_004__AddDeadlineColumnToWorkloadTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_41_003__AddBackfillConfigToSchemaManagementTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() {
    final DSLContext dslContext = getDslContext();
    final Set<String> workloadIndexesBeforeMigration = dslContext.select()
        .from(table("pg_indexes"))
        .where(field("tablename").eq("workload"))
        .fetch()
        .stream()
        .map(c -> c.getValue("indexname", String.class))
        .collect(Collectors.toSet());
    assertFalse(workloadIndexesBeforeMigration.contains("workload_deadline_idx"));

    V0_50_41_004__AddDeadlineColumnToWorkload.addDeadlineColumnToWorkload(dslContext);

    final Set<String> workloadIndexesAfterMigration = dslContext.select()
        .from(table("pg_indexes"))
        .where(field("tablename").eq("workload"))
        .fetch()
        .stream()
        .map(c -> c.getValue("indexname", String.class))
        .collect(Collectors.toSet());
    assertTrue(workloadIndexesAfterMigration.contains("workload_deadline_idx"));
  }

}
