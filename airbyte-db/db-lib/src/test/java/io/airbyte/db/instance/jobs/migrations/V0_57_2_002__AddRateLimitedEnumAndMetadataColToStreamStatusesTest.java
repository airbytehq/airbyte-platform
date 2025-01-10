/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import static java.util.stream.Collectors.toSet;
import static org.jooq.impl.DSL.field;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import io.airbyte.db.instance.jobs.AbstractJobsDatabaseTest;
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatusesTest extends AbstractJobsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_57_2_001__AddRefreshJobType", JobsDatabaseMigrator.DB_IDENTIFIER,
            JobsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final JobsDatabaseMigrator jobsDatabaseMigrator = new JobsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_57_2_001__AddRefreshJobType();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(jobsDatabaseMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() {
    final DSLContext context = getDslContext();
    Assertions.assertFalse(metadataColumnExists(context));
    Assertions.assertFalse(rateLimitedEnumExists(context));
    V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatuses.migrate(context);
    Assertions.assertTrue(metadataColumnExists(context));
    Assertions.assertTrue(rateLimitedEnumExists(context));
  }

  protected static boolean metadataColumnExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(field("table_name").eq("stream_statuses")
            .and(field("column_name").eq("metadata"))));
  }

  protected static boolean rateLimitedEnumExists(final DSLContext ctx) {
    return ctx.resultQuery("SELECT enumlabel FROM pg_enum WHERE enumtypid = 'job_stream_status_run_state'::regtype")
        .fetch()
        .stream()
        .map(c -> c.getValue("enumlabel"))
        .collect(toSet())
        .contains("rate_limited");
  }

}
