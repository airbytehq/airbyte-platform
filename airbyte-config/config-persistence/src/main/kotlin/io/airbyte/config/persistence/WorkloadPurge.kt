/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import io.airbyte.db.instance.configs.jooq.generated.Tables.WORKLOAD
import io.airbyte.db.instance.configs.jooq.generated.Tables.WORKLOAD_QUEUE
import io.airbyte.db.instance.configs.jooq.generated.enums.WorkloadStatus
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPurgeBatchSize
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset

private val log = KotlinLogging.logger {}

@Singleton
class WorkloadPurge(
  @param:Named("config") private val transactions: TransactionOperations<Connection>,
  private val featureFlagClient: FeatureFlagClient,
  @param:Named("workloadPurgeRetryPolicy") private val retryPolicy: RetryPolicy<Int> = WorkloadPurgeRetryPolicy().workloadPurgeRetryPolicy(),
) {
  /** Each batch commits independently, so a later failure preserves completed batches. */
  fun prune(now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)): Int {
    val cutoff = now.minusMonths(6)
    var totalDeleted = 0
    var deleted: Int
    do {
      deleted =
        Failsafe.with(retryPolicy).get { _ ->
          val batchSize = featureFlagClient.intVariation(WorkloadPurgeBatchSize, Empty)
          require(batchSize > 0) { "Workload purge batch size must be positive: $batchSize" }

          transactions.executeWrite { transaction ->
            val ctx = DSL.using(transaction.connection, SQLDialect.POSTGRES)
            ctx.execute("SET LOCAL statement_timeout = '1200s'")
            val ids =
              ctx
                .select(WORKLOAD.ID)
                .from(WORKLOAD)
                .where(WORKLOAD.STATUS.`in`(WorkloadStatus.success, WorkloadStatus.failure, WorkloadStatus.cancelled))
                .and(WORKLOAD.UPDATED_AT.lessThan(cutoff))
                .limit(batchSize)
                .forUpdate()
                .fetch(WORKLOAD.ID)
            deleteRows(ctx, ids)
          }
        }
      totalDeleted += deleted
      if (deleted > 0) {
        log.info { "Deleted $deleted workloads in batch; total deleted: $totalDeleted" }
      }
    } while (deleted > 0)
    return totalDeleted
  }

  /** DSR supplies exact IDs captured before job pruning; no workload prefix scan is needed. */
  fun deleteByIds(workloadIds: List<String>): Int {
    if (workloadIds.isEmpty()) {
      return 0
    }
    return transactions.executeWrite { transaction ->
      val ctx = DSL.using(transaction.connection, SQLDialect.POSTGRES)
      ctx.execute("SET LOCAL statement_timeout = '1200s'")
      deleteRows(ctx, workloadIds.distinct())
    }
  }

  private fun deleteRows(
    ctx: DSLContext,
    ids: List<String>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    ctx.deleteFrom(WORKLOAD_QUEUE).where(WORKLOAD_QUEUE.WORKLOAD_ID.`in`(ids)).execute()
    // commands.workload_id is ON DELETE CASCADE.
    return ctx.deleteFrom(WORKLOAD).where(WORKLOAD.ID.`in`(ids)).execute()
  }
}
