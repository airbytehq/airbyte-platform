/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class DeclarativeSourceUpdaterTest {
  private var mDeclarativeManifestImageVersionsProvider: RemoteDeclarativeManifestImageVersionsProvider = mockk()
  private var mDeclarativeManifestImageVersionService: DeclarativeManifestImageVersionService = mockk()
  private var mActorDefinitionService: ActorDefinitionService = mockk()
  private lateinit var declarativeSourceUpdater: DeclarativeSourceUpdater

  @BeforeEach
  fun setup() {
    declarativeSourceUpdater =
      DeclarativeSourceUpdater(
        mDeclarativeManifestImageVersionsProvider,
        mDeclarativeManifestImageVersionService,
        mActorDefinitionService,
      )

    justRun { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(any(), any()) }
    justRun { mActorDefinitionService.updateDeclarativeActorDefinitionVersions(any(), any()) }
    every {
      mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(any(), any())
    } returns DeclarativeManifestImageVersion(0, "0.1.0")
    every { mActorDefinitionService.updateDeclarativeActorDefinitionVersions(any(), any()) } returns 1
  }

  @Test
  fun `cdk versions are added when they are not in the database`() {
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf()
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns mapOf(0 to "0.1.0", 1 to "1.0.0")

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(0, "0.1.0") }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(1, "1.0.0") }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `new cdk versions are added to database and actor definitions are updated`() {
    every {
      mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions()
    } returns listOf(DeclarativeManifestImageVersion(0, "0.1.0"), DeclarativeManifestImageVersion(1, "1.0.0"))
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns mapOf(0 to "0.1.0", 1 to "1.0.1")

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(1, "1.0.1") }
    verify(exactly = 1) { mActorDefinitionService.updateDeclarativeActorDefinitionVersions("1.0.0", "1.0.1") }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `same cdk versions do not result in any calls to actor definition service`() {
    every {
      mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions()
    } returns listOf(DeclarativeManifestImageVersion(0, "0.1.0"), DeclarativeManifestImageVersion(1, "1.0.0"))
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns mapOf(0 to "0.1.0", 1 to "1.0.0")

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }
}
