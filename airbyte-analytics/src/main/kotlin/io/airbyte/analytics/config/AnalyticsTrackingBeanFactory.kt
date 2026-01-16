/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics.config

import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import io.airbyte.config.Organization
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

val BLANK_UUID = UUID(0, 0)

@Factory
class AnalyticsTrackingBeanFactory {
  @Singleton
  @Named("deploymentSupplier")
  fun deploymentSupplier(
    airbyteVersion: AirbyteVersion,
    airbyteEdition: Configs.AirbyteEdition,
  ): Supplier<DeploymentMetadataRead> =
    Supplier {
      DeploymentMetadataRead(
        id = BLANK_UUID,
        mode = airbyteEdition.name,
        version = airbyteVersion.serialize(),
      )
    }

  @Singleton
  @Named("workspaceFetcher")
  fun workspaceFetcher(): Function<UUID, WorkspaceRead> =
    Function { workspaceId: UUID ->
      WorkspaceRead(
        workspaceId = workspaceId,
        customerId = workspaceId,
        name = "",
        slug = "",
        initialSetupComplete = true,
        organizationId = workspaceId,
      )
    }

  @Singleton
  @Named("organizationFetcher")
  fun organizationFetcher(): Function<UUID, Organization> =
    Function { organizationId: UUID ->
      Organization().withOrganizationId(organizationId).withName("").withEmail("")
    }
}
