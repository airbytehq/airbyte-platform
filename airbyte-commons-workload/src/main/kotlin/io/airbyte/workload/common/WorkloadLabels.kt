/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.common

class WorkloadLabels {
  companion object {
    const val ACTOR_TYPE = "actor_type"
    const val ATTEMPT_LABEL_KEY = "attempt_id"
    const val CONNECTION_ID_LABEL_KEY = "connection_id"
    const val JOB_LABEL_KEY = "job_id"
    const val WORKER_POD_LABEL_KEY = "airbyte"
    const val WORKER_POD_LABEL_VALUE = "job-pod"
    const val WORKSPACE_LABEL_KEY = "workspace_id"
  }
}
