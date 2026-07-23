/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_031__AddExecutionCountsToDataSubjectDeletionRequestTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_031__AddExecutionCountsToDataSubjectDeletionRequestTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_030__AllowOrganizationMemberInvitations()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
  }

  @Test
  fun `adds nullable JSONB execution counts column`() {
    V2_1_0_031__AddExecutionCountsToDataSubjectDeletionRequest.addExecutionCountsColumn(ctx)

    val executionCountsField =
      ctx
        .meta()
        .getTables("data_subject_deletion_request")
        .first()
        .fields()
        .find { it.name == "execution_counts" }

    assertNotNull(executionCountsField, "execution_counts column should exist")
    assertEquals(SQLDataType.JSONB, executionCountsField?.dataType, "execution_counts column should be JSONB")

    val requestId = UUID.randomUUID()
    ctx.execute(
      """
      INSERT INTO data_subject_deletion_request (
        id,
        email,
        email_hash,
        datagrail_id,
        status,
        requested_by,
        oncall_issue_number,
        manifest,
        execution_counts
      )
      VALUES (?, 'dg-123', 'hash', 'dg-123', 'completed', 'support', 'ONCALL-123', '{}'::jsonb, ?::jsonb)
      """.trimIndent(),
      requestId,
      """{"deleted_jobs_count": 7, "tombstoned_user": true}""",
    )

    val deletedJobsCount =
      ctx.fetchValue(
        "SELECT (execution_counts ->> 'deleted_jobs_count')::int FROM data_subject_deletion_request WHERE id = ?",
        requestId,
      )

    assertEquals(7, deletedJobsCount)
  }
}
