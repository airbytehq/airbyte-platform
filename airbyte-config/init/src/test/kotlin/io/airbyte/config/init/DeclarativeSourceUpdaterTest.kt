/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.config.persistence.ConfigRepository
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
  private var mConfigRepository: ConfigRepository = mockk()
  private var mCdkVersionProvider: CdkVersionProvider = mockk()
  private lateinit var declarativeSourceUpdater: DeclarativeSourceUpdater

  @BeforeEach
  fun setup() {
    every { mCdkVersionProvider.cdkVersion } returns "1.2.3"

    declarativeSourceUpdater = DeclarativeSourceUpdater(mConfigRepository, mCdkVersionProvider)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun `definitions are updated when they're using the active declarative manifest`() {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    every { mConfigRepository.actorDefinitionIdsWithActiveDeclarativeManifest } returns Stream.of(id1, id2)
    every { mConfigRepository.updateActorDefinitionsDockerImageTag(any(), any()) } returns 2
    declarativeSourceUpdater.apply()
    verify { mConfigRepository.updateActorDefinitionsDockerImageTag(listOf(id1, id2), "1.2.3") }
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun `no definitions are updated if none are using the active declarative manifest`() {
    every { mConfigRepository.actorDefinitionIdsWithActiveDeclarativeManifest } returns Stream.empty()
    declarativeSourceUpdater.apply()
    verify(exactly = 0) { mConfigRepository.updateActorDefinitionsDockerImageTag(any(), any()) }
  }
}
