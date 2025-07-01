/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class StateCheckSumCountEvent
  @JsonCreator
  constructor(
    @JsonProperty("airbyte_version") val airbyteVersion: String?,
    @JsonProperty("attempt_number") val attemptNumber: Long,
    @JsonProperty("connection_id") val connectionId: String,
    @JsonProperty("deployment_id") val deploymentId: String?,
    @JsonProperty("deployment_mode") val deploymentMode: String?,
    @JsonProperty("email") val email: String?,
    @JsonProperty("id") val id: String,
    @JsonProperty("job_id") val jobId: Long,
    @JsonProperty("record_count") val recordCount: Long,
    @JsonProperty("state_hash") val stateHash: String,
    @JsonProperty("state_id") val stateId: String,
    @JsonProperty("state_origin") val stateOrigin: String,
    @JsonProperty("state_type") val stateType: String,
    @JsonProperty("stream_name") val streamName: String?,
    @JsonProperty("stream_namespace") val streamNamespace: String?,
    @JsonProperty("timestamp") val timestamp: Long,
    @JsonProperty("valid_data") val validData: Boolean,
  )
