package io.airbyte.data.services.impls.data.mappers

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.JobOutput
import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus
import java.nio.file.Path

typealias EntityAttempt = Attempt
typealias ModelAttempt = io.airbyte.config.Attempt

typealias EntityAttemptStatus = AttemptStatus
typealias ModelAttemptStatus = io.airbyte.config.AttemptStatus

fun EntityAttempt.toConfigModel(): ModelAttempt {
  return ModelAttempt(
    this.attemptNumber?.toInt() ?: 0,
    this.jobId!!,
    this.logPath?.let { Path.of(it) },
    Jsons.`object`(this.attemptSyncConfig, AttemptSyncConfig::class.java),
    Jsons.`object`(this.output, JobOutput::class.java),
    this.status?.toConfig()!!,
    this.processingTaskQueue,
    Jsons.`object`(this.failureSummary, AttemptFailureSummary::class.java),
    this.createdAt?.toEpochSecond() ?: 0,
    this.updatedAt?.toEpochSecond() ?: 0,
    this.endedAt?.toEpochSecond() ?: 0,
  )
}

fun EntityAttemptStatus.toConfig(): ModelAttemptStatus {
  return when (this) {
    AttemptStatus.running -> ModelAttemptStatus.RUNNING
    AttemptStatus.succeeded -> ModelAttemptStatus.SUCCEEDED
    AttemptStatus.failed -> ModelAttemptStatus.FAILED
  }
}

fun ModelAttemptStatus.toEntity(): EntityAttemptStatus {
  return when (this) {
    ModelAttemptStatus.RUNNING -> AttemptStatus.running
    ModelAttemptStatus.SUCCEEDED -> AttemptStatus.succeeded
    ModelAttemptStatus.FAILED -> AttemptStatus.failed
  }
}
