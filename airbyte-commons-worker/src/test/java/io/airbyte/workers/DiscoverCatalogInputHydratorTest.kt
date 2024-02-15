package io.airbyte.workers

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.config.ActorContext
import io.airbyte.config.StandardDiscoverCatalogInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class DiscoverCatalogInputHydratorTest {
  @Test
  fun `hydrates from base hydrator and copies expected values over`() {
    val base: BaseInputHydrator = mockk()

    val hydrator = DiscoverCatalogInputHydrator(base)

    val unhyrdatedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()
    val input =
      StandardDiscoverCatalogInput()
        .withActorContext(ActorContext().withOrganizationId(orgId))
        .withConfigHash(UUID.randomUUID().toString())
        .withSourceId(UUID.randomUUID().toString())
        .withConnectionConfiguration(unhyrdatedConfig)

    every { base.hydrateConfig(unhyrdatedConfig, orgId) } returns hydratedConfig

    val result = hydrator.getHydratedStandardDiscoverInput(input)

    Assertions.assertEquals(input.actorContext, result.actorContext)
    Assertions.assertEquals(input.configHash, result.configHash)
    Assertions.assertEquals(input.sourceId, result.sourceId)
    Assertions.assertEquals(hydratedConfig, result.connectionConfiguration)
  }
}
