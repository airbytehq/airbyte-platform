/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.WorkloadOutputApi
import io.airbyte.api.model.generated.WorkloadOutputWriteRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.storage.StorageClient
import io.airbyte.workload.services.WorkloadService
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.inject.Named

// This is wired up awkwardly because of the way our generated code works.
// For 1 method controllers, instead of putting the path on the method and the base on the controller,
// it generates it all as one path and slaps it on the controller.
// TODO: remove `/write` once another method/endpoint is added
@Controller("/api/v1/workload_output/write")
class WorkloadOutputController(
  @Named("outputDocumentStore") val storageClient: StorageClient,
  val workloadService: WorkloadService,
  val roleResolver: RoleResolver,
) : WorkloadOutputApi {
  @Status(HttpStatus.NO_CONTENT)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @Post // TODO: add `/write` once another method/endpoint is added
  override fun writeWorkloadOutput(req: WorkloadOutputWriteRequest) {
    val workload = workloadService.getWorkload(req.workloadId)

    val auth =
      roleResolver
        .newRequest()
        .withCurrentAuthentication()

    if (workload.organizationId != null) {
      auth.withOrg(workload.organizationId!!)
    }
    if (workload.workspaceId != null) {
      auth.withRef(AuthenticationId.WORKSPACE_ID, workload.workspaceId!!)
    }

    auth.requireRole(AuthRoleConstants.DATAPLANE)
    storageClient.write(req.workloadId, req.output)
  }
}
