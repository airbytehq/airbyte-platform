/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

@Factory
class AnalyticsTrackingBeanFactory {
  @Singleton
  @Named("deploymentSupplier")
  @Requires(bean = AirbyteApiClient::class)
  @Replaces(named = "deploymentSupplier")
  fun deploymentSupplier(airbyteApiClient: AirbyteApiClient): Supplier<DeploymentMetadataRead> {
    return Supplier { airbyteApiClient.deploymentMetadataApi.getDeploymentMetadata() }
  }

  @Singleton
  @Named("workspaceFetcher")
  @Requires(bean = AirbyteApiClient::class)
  @Replaces(named = "workspaceFetcher")
  fun workspaceFetcher(airbyteApiClient: AirbyteApiClient): Function<UUID, WorkspaceRead> {
    return Function {
        workspaceId: UUID? ->
      workspaceId.let { wid ->
        airbyteApiClient.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId = wid!!, includeTombstone = true))
      }
    }
  }
}
