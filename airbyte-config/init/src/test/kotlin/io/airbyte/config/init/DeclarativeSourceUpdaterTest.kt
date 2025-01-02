/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.RunDeclarativeSourcesUpdater
import io.airbyte.featureflag.Workspace
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class DeclarativeSourceUpdaterTest {
  private var testSha: String = "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"
  private var mDeclarativeManifestImageVersionsProvider: RemoteDeclarativeManifestImageVersionsProvider = mockk()
  private var mDeclarativeManifestImageVersionService: DeclarativeManifestImageVersionService = mockk()
  private var mActorDefinitionService: ActorDefinitionService = mockk()
  private var airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator = mockk()
  private var featureFlagClient: FeatureFlagClient = mockk()
  private lateinit var declarativeSourceUpdater: DeclarativeSourceUpdater

  @BeforeEach
  fun setup() {
    declarativeSourceUpdater =
      DeclarativeSourceUpdater(
        mDeclarativeManifestImageVersionsProvider,
        mDeclarativeManifestImageVersionService,
        mActorDefinitionService,
        airbyteCompatibleConnectorsValidator,
        featureFlagClient,
      )

    justRun { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(any()) }
    justRun { mActorDefinitionService.updateDeclarativeActorDefinitionVersions(any(), any()) }
    every {
      mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(any())
    } returns DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    every { mActorDefinitionService.updateDeclarativeActorDefinitionVersions(any(), any()) } returns 1
    every { airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(any()) } returns ConnectorPlatformCompatibilityValidationResult(true, "")
    every { featureFlagClient.boolVariation(RunDeclarativeSourcesUpdater, Workspace(ANONYMOUS)) } returns true
  }

  @Test
  fun `cdk versions are added when they are not in the database`() {
    val newVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    val newVersion1 = DeclarativeManifestImageVersion(1, "1.0.0", testSha)

    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf()
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(newVersion0, newVersion1)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion0) }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion1) }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `new sdm versions are added to database and actor definitions are updated`() {
    val oldVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    val oldVersion1 = DeclarativeManifestImageVersion(1, "1.0.0", testSha)
    val newVersion1 = DeclarativeManifestImageVersion(1, "1.0.1", testSha)
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf(oldVersion0, oldVersion1)
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(oldVersion0, newVersion1)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion1) }
    verify(exactly = 1) { mActorDefinitionService.updateDeclarativeActorDefinitionVersions("1.0.0", "1.0.1") }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `versions are rolled back if the image in the db is no longer found in docker hub`() {
    val olderVersion1 = DeclarativeManifestImageVersion(1, "1.0.0", testSha)
    val laterVersion1 = DeclarativeManifestImageVersion(1, "1.0.1", testSha)
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf(laterVersion1)
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(olderVersion1)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(olderVersion1) }
    verify(exactly = 1) { mActorDefinitionService.updateDeclarativeActorDefinitionVersions("1.0.1", "1.0.0") }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `same declarative manifest versions do not result in any calls to actor definition service`() {
    val oldVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    val oldVersion1 = DeclarativeManifestImageVersion(1, "1.0.0", testSha)
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf(oldVersion0, oldVersion1)
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(oldVersion0, oldVersion1)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `should update persisted version if SHA has changed`() {
    val oldVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    val newVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", "newSha")
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf(oldVersion0)
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(newVersion0)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion0) }
    verify(exactly = 0) { mActorDefinitionService.updateDeclarativeActorDefinitionVersions(any(), any()) }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `should not update version if validation returns false`() {
    val oldVersion0 = DeclarativeManifestImageVersion(0, "0.1.0", testSha)
    val oldVersion1 = DeclarativeManifestImageVersion(1, "1.0.0", testSha)
    val newVersion1 = DeclarativeManifestImageVersion(1, "1.0.1", testSha)
    every {
      airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(any())
    } returns ConnectorPlatformCompatibilityValidationResult(false, "Can't update definition")
    every { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() } returns listOf(oldVersion0, oldVersion1)
    every { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() } returns listOf(oldVersion0, newVersion1)

    declarativeSourceUpdater.apply()

    verify(exactly = 1) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 1) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    verify(exactly = 0) { mDeclarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion1) }
    verify(exactly = 0) { mActorDefinitionService.updateDeclarativeActorDefinitionVersions("1.0.0", "1.0.1") }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }

  @Test
  fun `feature flag should turn off behavior`() {
    every { featureFlagClient.boolVariation(RunDeclarativeSourcesUpdater, Workspace(ANONYMOUS)) } returns false
    declarativeSourceUpdater.apply()

    verify(exactly = 0) { mDeclarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions() }
    verify(exactly = 0) { mDeclarativeManifestImageVersionService.listDeclarativeManifestImageVersions() }
    confirmVerified(mDeclarativeManifestImageVersionService, mActorDefinitionService, mDeclarativeManifestImageVersionsProvider)
  }
}
