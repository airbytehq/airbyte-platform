/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.hydration

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.config.ActorContext
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class DiscoverCatalogInputHydratorTest {
  @Test
  fun `hydrates from base hydrator and copies expected values over`() {
    val base: ConnectorSecretsHydrator = mockk()

    val hydrator = DiscoverCatalogInputHydrator(base)

    val unhydratedConfig = InlinedConfigWithSecretRefs(POJONode("un-hydrated"))
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val input =
      StandardDiscoverCatalogInput()
        .withActorContext(ActorContext().withWorkspaceId(workspaceId).withOrganizationId(orgId))
        .withConfigHash(UUID.randomUUID().toString())
        .withConnectorVersion("1.0.0")
        .withSourceId(UUID.randomUUID().toString())
        .withConnectionConfiguration(unhydratedConfig.value)

    every {
      base.hydrateConfig(
        unhydratedConfig.toConfigWithRefs(),
        SecretHydrationContext(
          organizationId = orgId,
          workspaceId = workspaceId,
        ),
      )
    } returns hydratedConfig

    val result = hydrator.getHydratedStandardDiscoverInput(input)

    Assertions.assertEquals(input.actorContext, result.actorContext)
    Assertions.assertEquals(input.configHash, result.configHash)
    Assertions.assertEquals(input.connectorVersion, result.connectorVersion)
    Assertions.assertEquals(input.sourceId, result.sourceId)
    Assertions.assertEquals(hydratedConfig, result.connectionConfiguration)
  }
}
