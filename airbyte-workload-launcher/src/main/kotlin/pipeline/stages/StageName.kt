package io.airbyte.workload.launcher.pipeline.stages

enum class StageName {
  CLAIM,
  CHECK_STATUS,
  BUILD,
  MUTEX,
  LAUNCH,
}
