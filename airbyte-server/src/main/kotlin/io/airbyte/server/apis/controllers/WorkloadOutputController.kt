/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.WorkloadOutputApi
import io.airbyte.api.model.generated.WorkloadOutputWriteRequest
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.storage.StorageClient
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import jakarta.inject.Named

// This is wired up awkwardly because of the way our generated code works.
// For 1 method controllers, instead of putting the path on the method and the base on the controller,
// it generates it all as one path and slaps it on the controller.
// TODO: remove `/write` once another method/endpoint is added
@Controller("/api/v1/workload_output/write")
class WorkloadOutputController(
  @Named("outputDocumentStore") val storageClient: StorageClient,
) : WorkloadOutputApi {
  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post // TODO: add `/write` once another method/endpoint is added
  override fun writeWorkloadOutput(req: WorkloadOutputWriteRequest) {
    storageClient.write(req.workloadId, req.output)
  }
}
