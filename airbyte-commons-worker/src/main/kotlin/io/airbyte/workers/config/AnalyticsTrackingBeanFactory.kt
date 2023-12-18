/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.airbyte.api.client.generated.DeploymentMetadataApi
import io.airbyte.api.client.generated.WorkspaceApi
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
  @Requires(bean = DeploymentMetadataApi::class)
  @Replaces(named = "deploymentSupplier")
  fun deploymentSupplier(deploymentMetadataApi: DeploymentMetadataApi): Supplier<DeploymentMetadataRead> {
    return Supplier { deploymentMetadataApi.getDeploymentMetadata() }
  }

  @Singleton
  @Named("workspaceFetcher")
  @Requires(bean = WorkspaceApi::class)
  @Replaces(named = "workspaceFetcher")
  fun workspaceFetcher(workspaceApi: WorkspaceApi): Function<UUID, WorkspaceRead> {
    return Function { workspaceId: UUID? -> workspaceApi.getWorkspace(WorkspaceIdRequestBody().workspaceId(workspaceId).includeTombstone(true)) }
  }
}
