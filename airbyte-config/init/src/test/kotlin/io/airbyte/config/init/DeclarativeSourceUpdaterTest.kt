/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID
import java.util.stream.Stream

internal class DeclarativeSourceUpdaterTest {
  private var mConnectorBuilderService: ConnectorBuilderService = mockk()
  private var mActorDefinitionService: ActorDefinitionService = mockk()
  private var mCdkVersionProvider: CdkVersionProvider = mockk()
  private lateinit var declarativeSourceUpdater: DeclarativeSourceUpdater

  @BeforeEach
  fun setup() {
    every { mCdkVersionProvider.cdkVersion } returns "1.2.3"

    declarativeSourceUpdater = DeclarativeSourceUpdater(mConnectorBuilderService, mActorDefinitionService, mCdkVersionProvider)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun `definitions are updated when they're using the active declarative manifest`() {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    every { mConnectorBuilderService.actorDefinitionIdsWithActiveDeclarativeManifest } returns Stream.of(id1, id2)
    every { mActorDefinitionService.updateActorDefinitionsDockerImageTag(any(), any()) } returns 2
    declarativeSourceUpdater.apply()
    verify { mActorDefinitionService.updateActorDefinitionsDockerImageTag(listOf(id1, id2), "1.2.3") }
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun `no definitions are updated if none are using the active declarative manifest`() {
    every { mConnectorBuilderService.actorDefinitionIdsWithActiveDeclarativeManifest } returns Stream.empty()
    declarativeSourceUpdater.apply()
    verify(exactly = 0) { mActorDefinitionService.updateActorDefinitionsDockerImageTag(any(), any()) }
  }
}
