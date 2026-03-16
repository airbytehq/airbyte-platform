/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import kotlin.jvm.javaClass

private val log = KotlinLogging.logger {}

/**
 * Add 'queued' status to job_status enum for Data Worker capacity enforcement.
 *
 * When committed capacity is exhausted, jobs are created with QUEUED status
 * and wait for capacity to become available before transitioning to PENDING.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_002__AddQueuedJobStatus : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.alterType("job_status").addValue("queued").execute()
  }
}
