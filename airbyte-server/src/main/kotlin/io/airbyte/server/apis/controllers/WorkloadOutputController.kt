/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.WorkloadOutputApi
import io.airbyte.api.server.generated.models.WorkloadOutputWriteRequest
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.storage.StorageClient
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.WorkloadOutputRule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.inject.Named

// This is wired up awkwardly because of the way our generated code works.
// For 1 method controllers, instead of putting the path on the method and the base on the controller,
// it generates it all as one path and slaps it on the controller.
// TODO: remove `/write` once another method/endpoint is added
@Controller
class WorkloadOutputController(
  @Named("outputDocumentStore") val storageClient: StorageClient,
) : WorkloadOutputApi {
  @Status(HttpStatus.NO_CONTENT)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WorkloadOutputRule::class,
  )
  override fun writeWorkloadOutput(workloadOutputWriteRequest: WorkloadOutputWriteRequest?) {
    val req = workloadOutputWriteRequest.requireBody()
    storageClient.write(req.workloadId, req.output)
  }
}
