package io.airbyte.workers

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class CheckConnectionInputHydratorTest {
  @Test
  fun `hydrates from base hydrator and copies expected values over`() {
    val base: ConnectorSecretsHydrator = mockk()

    val hydrator = CheckConnectionInputHydrator(base)

    val unhydratedConfig = POJONode("un-hydrated")
    val hydratedConfig = POJONode("hydrated")

    val orgId = UUID.randomUUID()
    val input =
      StandardCheckConnectionInput()
        .withActorContext(ActorContext().withOrganizationId(orgId))
        .withActorType(ActorType.DESTINATION)
        .withActorId(UUID.randomUUID())
        .withConnectionConfiguration(unhydratedConfig)

    every { base.hydrateConfig(unhydratedConfig, orgId) } returns hydratedConfig

    val result = hydrator.getHydratedStandardCheckInput(input)

    Assertions.assertEquals(input.actorContext, result.actorContext)
    Assertions.assertEquals(input.actorId, result.actorId)
    Assertions.assertEquals(input.actorType, result.actorType)
    Assertions.assertEquals(hydratedConfig, result.connectionConfiguration)
  }
}
