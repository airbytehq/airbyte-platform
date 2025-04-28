/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.Metadata
import io.airbyte.db.instance.jobs.AbstractJobsDatabaseTest
import io.airbyte.db.instance.jobs.migrations.V0_35_40_001__MigrateFailureReasonEnumValues.AttemptFailureSummaryForMigration
import io.airbyte.db.instance.jobs.migrations.V0_35_40_001__MigrateFailureReasonEnumValues.Companion.updateRecordsWithNewEnumValues
import io.airbyte.db.instance.jobs.migrations.V0_35_40_001__MigrateFailureReasonEnumValues.FailureReasonForMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V0_35_40_001_MigrateFailureReasonEnumValues_Test : AbstractJobsDatabaseTest() {
  @Test
  fun test() {
    val ctx = getDslContext()

    V0_35_5_001__Add_failureSummary_col_to_Attempts.migrate(ctx)

    addRecordsWithOldEnumValues(ctx)

    updateRecordsWithNewEnumValues(ctx)

    verifyEnumValuesFixed(ctx)
  }

  companion object {
    private var currJobId = 1
    private val timeNowMillis = System.currentTimeMillis()
    private const val ORIGIN_SOURCE = "source"

    // create pairs of old failure reasons and their fixed versions.
    private val originReplicationWorker = baseFailureReason().withFailureOrigin(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_REPLICATION_ORIGIN)
    private val fixedOriginReplicationWorker =
      baseFailureReason().withFailureOrigin(
        V0_35_40_001__MigrateFailureReasonEnumValues.NEW_REPLICATION_ORIGIN,
      )

    private val originUnknown = baseFailureReason().withFailureOrigin(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_UNKNOWN)
    private val fixedOriginUnknown = baseFailureReason().withFailureOrigin(null)

    private val typeManualCancellation = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_MANUAL_CANCELLATION)
    private val fixedTypeManualCancellation =
      baseFailureReason().withFailureType(
        V0_35_40_001__MigrateFailureReasonEnumValues.NEW_MANUAL_CANCELLATION,
      )

    private val typeSystemError = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_SYSTEM_ERROR)
    private val fixedTypeSystemError = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.NEW_SYSTEM_ERROR)
    private val typeConfigError = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_CONFIG_ERROR)
    private val fixedTypeConfigError = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.NEW_CONFIG_ERROR)

    private val typeUnknown = baseFailureReason().withFailureType(V0_35_40_001__MigrateFailureReasonEnumValues.OLD_UNKNOWN)
    private val fixedTypeUnknown = baseFailureReason().withFailureType(null)

    // enum values that don't need updating, or aren't recognized at all, should be left untouched
    private val noChangeNeeded = baseFailureReason().withFailureOrigin(ORIGIN_SOURCE)
    private val unrecognizedValue = baseFailureReason().withFailureType("someUnrecognizedValue")

    // create failure summaries containing failure reasons that need fixing.
    // mixing in noChangeNeeded reasons in different spots to make sure the migration properly leaves
    // those untouched.
    private val summaryFixReplicationOrigin = getFailureSummary(noChangeNeeded, originReplicationWorker)
    private val summaryFixReplicationOriginAndManualCancellationType =
      getFailureSummary(originReplicationWorker, typeManualCancellation, noChangeNeeded)
    private val summaryFixUnknownOriginAndUnknownType = getFailureSummary(originUnknown, noChangeNeeded, typeUnknown)
    private val summaryFixMultipleSystemErrorType = getFailureSummary(typeSystemError, typeSystemError)
    private val summaryFixConfigErrorType = getFailureSummary(typeConfigError)
    private val summaryNoChangeNeeded = getFailureSummary(noChangeNeeded, noChangeNeeded)
    private val summaryFixOriginAndLeaveUnrecognizedValue = getFailureSummary(originReplicationWorker, unrecognizedValue)

    // define attempt ids corresponding to each summary above
    private const val ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN = 1L
    private const val ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN_AND_MANUAL_CANCELLATION_TYPE = 2L
    private const val ATTEMPT_ID_FOR_FIX_UNKNOWN_ORIGIN_AND_UNKNOWN_TYPE = 3L
    private const val ATTEMPT_ID_FOR_FIX_MULTIPLE_SYSTEM_ERROR_TYPE = 4L
    private const val ATTEMPT_ID_FOR_FIX_CONFIG_ERROR_TYPE = 5L
    private const val ATTEMPT_ID_FOR_NO_CHANGE_NEEDED = 6L
    private const val ATTEMPT_ID_FOR_FIX_ORIGIN_AND_LEAVE_UNRECOGNIZED_VALUE = 7L

    // create expected fixed failure summaries after migration.
    private val expectedSummaryFixReplicationOrigin = getFailureSummary(noChangeNeeded, fixedOriginReplicationWorker)
    private val expectedSummaryFixReplicationOriginAndManualCancellationType =
      getFailureSummary(fixedOriginReplicationWorker, fixedTypeManualCancellation, noChangeNeeded)
    private val expectedSummaryFixUnknownOriginAndUnknownType = getFailureSummary(fixedOriginUnknown, noChangeNeeded, fixedTypeUnknown)
    private val expectedSummaryFixMultipleSystemErrorType = getFailureSummary(fixedTypeSystemError, fixedTypeSystemError)
    private val expectedSummaryFixConfigErrorType = getFailureSummary(fixedTypeConfigError)
    private val expectedSummaryNoChangeNeeded = getFailureSummary(noChangeNeeded, noChangeNeeded)
    private val expectedFixOriginAndLeaveUnrecognizedValue = getFailureSummary(fixedOriginReplicationWorker, unrecognizedValue)

    private fun addRecordsWithOldEnumValues(ctx: DSLContext) {
      insertAttemptWithSummary(ctx, ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN, summaryFixReplicationOrigin)
      insertAttemptWithSummary(
        ctx,
        ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN_AND_MANUAL_CANCELLATION_TYPE,
        summaryFixReplicationOriginAndManualCancellationType,
      )
      insertAttemptWithSummary(
        ctx,
        ATTEMPT_ID_FOR_FIX_UNKNOWN_ORIGIN_AND_UNKNOWN_TYPE,
        summaryFixUnknownOriginAndUnknownType,
      )
      insertAttemptWithSummary(ctx, ATTEMPT_ID_FOR_FIX_MULTIPLE_SYSTEM_ERROR_TYPE, summaryFixMultipleSystemErrorType)
      insertAttemptWithSummary(ctx, ATTEMPT_ID_FOR_FIX_CONFIG_ERROR_TYPE, summaryFixConfigErrorType)
      insertAttemptWithSummary(ctx, ATTEMPT_ID_FOR_NO_CHANGE_NEEDED, summaryNoChangeNeeded)
      insertAttemptWithSummary(
        ctx,
        ATTEMPT_ID_FOR_FIX_ORIGIN_AND_LEAVE_UNRECOGNIZED_VALUE,
        summaryFixOriginAndLeaveUnrecognizedValue,
      )
    }

    private fun verifyEnumValuesFixed(ctx: DSLContext) {
      assertEquals(
        expectedSummaryFixReplicationOrigin,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN),
      )
      assertEquals(
        expectedSummaryFixReplicationOriginAndManualCancellationType,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_REPLICATION_ORIGIN_AND_MANUAL_CANCELLATION_TYPE),
      )
      assertEquals(
        expectedSummaryFixUnknownOriginAndUnknownType,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_UNKNOWN_ORIGIN_AND_UNKNOWN_TYPE),
      )
      assertEquals(
        expectedSummaryFixMultipleSystemErrorType,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_MULTIPLE_SYSTEM_ERROR_TYPE),
      )
      assertEquals(
        expectedSummaryFixConfigErrorType,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_CONFIG_ERROR_TYPE),
      )
      assertEquals(expectedSummaryNoChangeNeeded, fetchFailureSummary(ctx, ATTEMPT_ID_FOR_NO_CHANGE_NEEDED))
      assertEquals(
        expectedFixOriginAndLeaveUnrecognizedValue,
        fetchFailureSummary(ctx, ATTEMPT_ID_FOR_FIX_ORIGIN_AND_LEAVE_UNRECOGNIZED_VALUE),
      )
    }

    private fun insertAttemptWithSummary(
      ctx: DSLContext,
      attemptId: Long,
      summary: AttemptFailureSummaryForMigration,
    ) {
      ctx
        .insertInto(DSL.table("attempts"))
        .columns(
          DSL.field("id"),
          DSL.field("failure_summary"),
          DSL.field("job_id"),
          DSL.field("attempt_number"),
        ).values(
          attemptId,
          JSONB.valueOf(Jsons.serialize(summary)),
          currJobId,
          1,
        ).execute()

      currJobId++
    }

    private fun fetchFailureSummary(
      ctx: DSLContext,
      attemptId: Long,
    ): AttemptFailureSummaryForMigration {
      val record =
        ctx.fetchOne(
          DSL
            .select(DSL.asterisk())
            .from(DSL.table("attempts"))
            .where(DSL.field("id").eq(attemptId)),
        )

      return Jsons.deserialize(
        record!!.get(DSL.field("failure_summary", SQLDataType.JSONB.nullable(true))).data(),
        AttemptFailureSummaryForMigration::class.java,
      )
    }

    private fun baseFailureReason(): FailureReasonForMigration =
      FailureReasonForMigration()
        .withInternalMessage("some internal message")
        .withExternalMessage("some external message")
        .withRetryable(false)
        .withTimestamp(timeNowMillis)
        .withStacktrace("some stacktrace")
        .withMetadata(Metadata().withAdditionalProperty("key1", "value1"))

    private fun getFailureSummary(vararg failureReasons: FailureReasonForMigration): AttemptFailureSummaryForMigration =
      AttemptFailureSummaryForMigration()
        .withPartialSuccess(false)
        .withFailures(listOf(*failureReasons))
  }
}
