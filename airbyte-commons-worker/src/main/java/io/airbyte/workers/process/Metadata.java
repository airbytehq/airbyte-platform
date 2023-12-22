/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

/**
 * The following variables help, either via names or labels, add metadata to processes actually
 * running operations to ease operations.
 */
public final class Metadata {

  /**
   * General Metadata.
   */
  public static final String JOB_LABEL_KEY = "job_id";
  public static final String ATTEMPT_LABEL_KEY = "attempt_id";
  public static final String WORKSPACE_LABEL_KEY = "workspace_id";
  public static final String WORKER_POD_LABEL_KEY = "airbyte";
  public static final String WORKER_POD_LABEL_VALUE = "job-pod";
  public static final String CONNECTION_ID_LABEL_KEY = "connection_id";
  public static final String IMAGE_NAME = "image_name";
  public static final String IMAGE_VERSION = "image_version";
  public static final String ACTOR_TYPE = "actor_type";

  /**
   * These are more readable forms of {@link io.airbyte.config.JobTypeResourceLimit.JobType}.
   */
  public static final String JOB_TYPE_KEY = "job_type";
  public static final String SYNC_JOB = "sync";
  public static final String SPEC_JOB = "spec";
  public static final String CHECK_JOB = "check";
  public static final String DISCOVER_JOB = "discover";

  /**
   * A sync job can actually be broken down into the following steps. Try to be as precise as possible
   * with naming/labels to help operations.
   */
  public static final String SYNC_STEP_KEY = "sync_step";
  public static final String READ_STEP = "read";
  public static final String WRITE_STEP = "write";
  public static final String NORMALIZE_STEP = "normalize";
  public static final String CUSTOM_STEP = "custom";
  public static final String ORCHESTRATOR_NORMALIZATION_STEP = "orchestrator-normalization";
  public static final String ORCHESTRATOR_DBT_NORMALIZATION_STEP = "orchestrator-dbt-normalization";
  public static final String ORCHESTRATOR_REPLICATION_STEP = "orchestrator-replication";

  public static final String CHECK_STEP_KEY = "check_step";
  public static final String CONNECTOR_STEP = "connector";
  public static final String ORCHESTRATOR_CHECK_STEP = "orchestrator-check";

}
