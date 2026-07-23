/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.DATA_SUBJECT_DELETION_REQUEST_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.DefaultDataType
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

/**
 * Creates the `data_subject_deletion_request` table used by the GDPR / DSR (Data Subject Request)
 * automation endpoints.
 *
 * The table tracks the two-phase deletion lifecycle:
 *  1. `PREVIEWED` — the read-only preview (`/api/v1/internal/dsr/preview`) captured the manifest
 *     that Support reviewed. No destructive action has run.
 *  2. `RUNNING` — execute has been authorized and the destructive runbook is in progress.
 *  3. `COMPLETED` — the hard-delete finished successfully.
 *  4. `FAILED` — the hard-delete encountered an unrecoverable error.
 *  5. `CANCELED` — the previewed request was canceled before being executed.
 *
 * A partial unique index prevents two active deletion requests from existing for the same target
 * email hash at the same time.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_028__CreateDataSubjectDeletionRequestTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    createDataSubjectDeletionStatusEnumType(ctx)
    createDataSubjectDeletionRequestTableAndIndexes(ctx)
  }

  /**
   * Lifecycle status for a DSR deletion request.
   */
  enum class DataSubjectDeletionStatus(
    private val literal: String,
  ) : EnumType {
    PREVIEWED("previewed"),
    RUNNING("running"),
    COMPLETED("completed"),
    FAILED("failed"),
    CANCELED("canceled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "data_subject_deletion_status"
    }
  }

  companion object {
    private fun createDataSubjectDeletionStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(DataSubjectDeletionStatus.NAME)
        .asEnum(*DataSubjectDeletionStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createDataSubjectDeletionRequestTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val email = DSL.field("email", SQLDataType.VARCHAR(512).nullable(false))
      val emailHash = DSL.field("email_hash", SQLDataType.VARCHAR(64).nullable(false))
      val datagrailId = DSL.field("datagrail_id", SQLDataType.VARCHAR(255).nullable(false))
      val status =
        DSL.field(
          "status",
          SQLDataType.VARCHAR
            .asEnumDataType(DataSubjectDeletionStatus::class.java)
            .nullable(false)
            .defaultValue(DataSubjectDeletionStatus.PREVIEWED),
        )
      val userId = DSL.field("user_id", SQLDataType.UUID.nullable(true))
      val requestedBy = DSL.field("requested_by", SQLDataType.VARCHAR(255).nullable(false))
      val oncallIssueNumber = DSL.field("oncall_issue_number", SQLDataType.VARCHAR(255).nullable(false))
      val confirmedBy = DSL.field("confirmed_by", SQLDataType.VARCHAR(255).nullable(true))
      val manifest = DSL.field("manifest", DefaultDataType(null, String::class.java, "jsonb").nullable(false))
      val prepareWarnings = DSL.field("prepare_warnings", DefaultDataType(null, String::class.java, "jsonb").nullable(true))
      val confirmErrors = DSL.field("confirm_errors", DefaultDataType(null, String::class.java, "jsonb").nullable(true))
      val preparedAt =
        DSL.field("prepared_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val confirmedAt = DSL.field("confirmed_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val completedAt = DSL.field("completed_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(DATA_SUBJECT_DELETION_REQUEST_TABLE)
        .columns(
          id,
          email,
          emailHash,
          datagrailId,
          status,
          userId,
          requestedBy,
          oncallIssueNumber,
          confirmedBy,
          manifest,
          prepareWarnings,
          confirmErrors,
          preparedAt,
          confirmedAt,
          completedAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
        ).execute()

      // Partial unique index: only one non-terminal (PREVIEWED / RUNNING) request per target-email
      // hash at a time. This makes the preview endpoint idempotent / safe to retry without
      // duplicating records, while still allowing historical COMPLETED / FAILED / CANCELED rows for
      // the same hash (a single user could conceivably be re-registered and re-deleted years later).
      ctx
        .execute(
          """
          CREATE UNIQUE INDEX IF NOT EXISTS idx_data_subject_deletion_request_email_active
          ON data_subject_deletion_request (email_hash)
          WHERE status IN ('previewed', 'running')
          """.trimIndent(),
        )

      ctx
        .createIndexIfNotExists("idx_data_subject_deletion_request_email_hash")
        .on(DATA_SUBJECT_DELETION_REQUEST_TABLE, "email_hash")
        .execute()

      ctx
        .createIndexIfNotExists("idx_data_subject_deletion_request_status")
        .on(DATA_SUBJECT_DELETION_REQUEST_TABLE, "status")
        .execute()

      ctx
        .createIndexIfNotExists("idx_data_subject_deletion_request_user_id")
        .on(DATA_SUBJECT_DELETION_REQUEST_TABLE, "user_id")
        .execute()
    }
  }
}
