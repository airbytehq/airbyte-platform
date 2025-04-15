/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Metadata
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add failure reasons migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_40_001__MigrateFailureReasonEnumValues : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    updateRecordsWithNewEnumValues(ctx)
  }

  /**
   * The following classes are essentially a copy of the FailureReason and AttemptFailureSummary
   * classes at the time of this migration. They support both deprecated and new enum values and are
   * used for record deserialization in this migration because in the future, the real FailureReason
   * class will have those deprecated enum values removed, which would break deserialization within
   * this migration.
   */
  @InternalForTesting
  data class FailureReasonForMigration(
    var failureOrigin: String? = null,
    var failureType: String? = null,
    var internalMessage: String? = null,
    var externalMessage: String? = null,
    var metadata: Metadata? = null,
    var stacktrace: String? = null,
    var retryable: Boolean? = null,
    var timestamp: Long? = null,
  ) {
    fun withFailureOrigin(failureOrigin: String?): FailureReasonForMigration = apply { this.failureOrigin = failureOrigin }

    fun withFailureType(failureType: String?): FailureReasonForMigration = apply { this.failureType = failureType }

    fun withInternalMessage(internalMessage: String?): FailureReasonForMigration = apply { this.internalMessage = internalMessage }

    fun withExternalMessage(externalMessage: String?): FailureReasonForMigration = apply { this.externalMessage = externalMessage }

    fun withMetadata(metadata: Metadata?): FailureReasonForMigration = apply { this.metadata = metadata }

    fun withStacktrace(stacktrace: String?): FailureReasonForMigration = apply { this.stacktrace = stacktrace }

    fun withRetryable(retryable: Boolean?): FailureReasonForMigration = apply { this.retryable = retryable }

    fun withTimestamp(timestamp: Long?): FailureReasonForMigration = apply { this.timestamp = timestamp }
  }

//  internal data class AttemptFailureSummaryForMigration(
//    val
//  )

  data class AttemptFailureSummaryForMigration(
    var failures: List<FailureReasonForMigration>? = ArrayList(),
    var partialSuccess: Boolean? = null,
  ) {
    fun withFailures(failures: List<FailureReasonForMigration>?) = apply { this.failures = failures }

    fun withPartialSuccess(partialSuccess: Boolean?): AttemptFailureSummaryForMigration = apply { this.partialSuccess = partialSuccess }
  }

  companion object {
    const val OLD_MANUAL_CANCELLATION: String = "manualCancellation"
    const val NEW_MANUAL_CANCELLATION: String = "manual_cancellation"
    const val OLD_SYSTEM_ERROR: String = "systemError"
    const val NEW_SYSTEM_ERROR: String = "system_error"
    const val OLD_CONFIG_ERROR: String = "configError"
    const val NEW_CONFIG_ERROR: String = "config_error"
    const val OLD_REPLICATION_ORIGIN: String = "replicationWorker"
    const val NEW_REPLICATION_ORIGIN: String = "replication"
    const val OLD_UNKNOWN: String = "unknown"

    /**
     * Finds all attempt record that have a failure summary containing a deprecated enum value. For each
     * record, calls method to fix and update.
     */
    @JvmStatic
    fun updateRecordsWithNewEnumValues(ctx: DSLContext) {
      val results =
        ctx.fetch(
          """
          SELECT A.* FROM attempts A, jsonb_array_elements(A.failure_summary->'failures') as f
          WHERE f->>'failureOrigin' = '$OLD_UNKNOWN'
          OR f->>'failureOrigin' = '$OLD_REPLICATION_ORIGIN'
          OR f->>'failureType' = '$OLD_UNKNOWN'
          OR f->>'failureType' = '$OLD_CONFIG_ERROR'
          OR f->>'failureType' = '$OLD_SYSTEM_ERROR'
          OR f->>'failureType' = '$OLD_MANUAL_CANCELLATION'
          """.trimIndent(),
        )

      results.forEach { updateAttemptFailureReasons(ctx, it) }
    }

    /**
     * Takes in a single record from the above query and performs an UPDATE to set the failure summary
     * to the fixed version.
     */
    private fun updateAttemptFailureReasons(
      ctx: DSLContext,
      record: Record,
    ) {
      val attemptIdField = DSL.field("id", SQLDataType.BIGINT)
      val failureSummaryField = DSL.field("failure_summary", SQLDataType.JSONB.nullable(true))

      val attemptId = record.get(attemptIdField)
      val oldFailureSummary =
        Jsons.deserialize(
          record.get(failureSummaryField).data(),
          AttemptFailureSummaryForMigration::class.java,
        )

      val fixedFailureSummary = getFixedAttemptFailureSummary(oldFailureSummary)

      ctx
        .update(DSL.table("attempts"))
        .set(failureSummaryField, JSONB.valueOf(Jsons.serialize(fixedFailureSummary)))
        .where(attemptIdField.eq(attemptId))
        .execute()
    }

    /**
     * Takes in a FailureSummary and replaces deprecated enum values with their updated versions.
     */
    private fun getFixedAttemptFailureSummary(failureSummary: AttemptFailureSummaryForMigration): AttemptFailureSummaryForMigration {
      val oldFailureTypeToFixedFailureType: Map<String, String> =
        mapOf(
          OLD_MANUAL_CANCELLATION to NEW_MANUAL_CANCELLATION,
          OLD_SYSTEM_ERROR to NEW_SYSTEM_ERROR,
          OLD_CONFIG_ERROR to NEW_CONFIG_ERROR,
        )

      val oldFailureOriginToFixedFailureOrigin: Map<String, String> = mapOf(OLD_REPLICATION_ORIGIN to NEW_REPLICATION_ORIGIN)

      val fixedFailureReasons = mutableListOf<FailureReasonForMigration>()

      failureSummary.failures!!.forEach { failureReason: FailureReasonForMigration ->
        val failureType = failureReason.failureType
        val failureOrigin = failureReason.failureOrigin

        // null failureType is valid and doesn't need correction
        failureType?.let {
          if (oldFailureTypeToFixedFailureType.containsKey(it)) {
            failureReason.failureType = oldFailureTypeToFixedFailureType[it]
          } else if (failureType == OLD_UNKNOWN) {
            failureReason.failureType = null
          }
        }

        // null failureOrigin is valid and doesn't need correction
        failureOrigin?.let {
          if (oldFailureOriginToFixedFailureOrigin.containsKey(it)) {
            failureReason.failureOrigin = oldFailureOriginToFixedFailureOrigin[it]
          } else if (failureOrigin == OLD_UNKNOWN) {
            failureReason.failureOrigin = null
          }
        }
        fixedFailureReasons.add(failureReason)
      }

      failureSummary.failures = fixedFailureReasons
      return failureSummary
    }
  }
}
