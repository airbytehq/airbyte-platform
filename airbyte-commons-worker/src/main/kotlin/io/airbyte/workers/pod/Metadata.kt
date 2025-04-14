/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod

/**
 * The following variables help, either via names or labels, add metadata to processes actually
 * running operations to ease operations.
 */
object Metadata {
  /** General Metadata. */
  const val JOB_LABEL_KEY = "job_id"
  const val ATTEMPT_LABEL_KEY = "attempt_id"
  const val WORKSPACE_LABEL_KEY = "workspace_id"
  const val WORKER_POD_LABEL_KEY = "airbyte"
  const val WORKER_POD_LABEL_VALUE = "job-pod"
  const val CONNECTION_ID_LABEL_KEY = "connection_id"
  const val ACTOR_ID_LABEL_KEY = "actor_id"
  const val IMAGE_NAME = "image_name"
  const val IMAGE_VERSION = "image_version"
  const val ACTOR_TYPE = "actor_type"

  /** These are more readable forms of [io.airbyte.config.JobTypeResourceLimit.JobType]. */
  const val JOB_TYPE_KEY = "job_type"
  const val SYNC_JOB = "sync"
  const val SPEC_JOB = "spec"
  const val CHECK_JOB = "check"
  const val DISCOVER_JOB = "discover"

  /** A sync job can actually be broken down into the following steps. Try to be as precise as possible with naming/labels to help operations. */
  const val SYNC_STEP_KEY = "sync_step"
  const val READ_STEP = "read"
  const val WRITE_STEP = "write"
  const val ORCHESTRATOR_REPLICATION_STEP = "orchestrator-replication"
  const val REPLICATION_STEP = "replication"
  const val AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
  const val AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
  const val AWS_ASSUME_ROLE_SECRET_NAME = "AWS_ASSUME_ROLE_SECRET_NAME"
  const val AWS_ASSUME_ROLE_EXTERNAL_ID = "AWS_ASSUME_ROLE_EXTERNAL_ID"
}
