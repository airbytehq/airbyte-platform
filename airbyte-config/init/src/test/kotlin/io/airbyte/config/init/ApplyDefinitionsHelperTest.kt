/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BreakingChanges
import io.airbyte.config.Configs.SeedDefinitionsProviderType
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ConnectorReleasesDestination
import io.airbyte.config.ConnectorReleasesSource
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ReleaseCandidatesDestination
import io.airbyte.config.ReleaseCandidatesSource
import io.airbyte.config.RolloutConfiguration
import io.airbyte.config.VersionBreakingChange
import io.airbyte.config.helpers.ConnectorRegistryConverters
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import io.micrometer.core.instrument.Counter
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

/**
 * Test suite for the [ApplyDefinitionsHelper] class.
 */
internal class ApplyDefinitionsHelperTest {
  private val definitionsProvider: DefinitionsProvider = mockk()
  private val jobPersistence: JobPersistence = mockk()
  private val actorDefinitionService: ActorDefinitionService = mockk()
  private val sourceService: SourceService = mockk()
  private val destinationService: DestinationService = mockk()
  private val metricClient: MetricClient = mockk()
  private val supportStateUpdater: SupportStateUpdater = mockk()
  private val actorDefinitionVersionResolver: ActorDefinitionVersionResolver = mockk()
  private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator = mockk()
  private val connectorRolloutService: ConnectorRolloutService = mockk()
  private val seedDefinitionsProviderType: SeedDefinitionsProviderType = mockk()
  private lateinit var applyDefinitionsHelper: ApplyDefinitionsHelper

  @BeforeEach
  fun setup() {
    applyDefinitionsHelper =
      ApplyDefinitionsHelper(
        definitionsProvider,
        seedDefinitionsProviderType,
        jobPersistence,
        actorDefinitionService,
        sourceService,
        destinationService,
        metricClient,
        supportStateUpdater,
        actorDefinitionVersionResolver,
        airbyteCompatibleConnectorsValidator,
        connectorRolloutService,
      )

    every { actorDefinitionService.getActorDefinitionIdsInUse() } returns emptySet()
    every { actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap() } returns emptyMap()
    every { definitionsProvider.getDestinationDefinitions() } returns emptyList()
    every { definitionsProvider.getSourceDefinitions() } returns emptyList()
    every { airbyteCompatibleConnectorsValidator.validate(any(), any()) } returns ConnectorPlatformCompatibilityValidationResult(true, null)
    every { jobPersistence.getCurrentProtocolVersionRange() } returns Optional.of(AirbyteProtocolVersionRange(Version("2.0.0"), Version("3.0.0")))
    every { actorDefinitionVersionResolver.fetchRemoteActorDefinitionVersion(any(), any(), any()) } returns Optional.empty()
    every { seedDefinitionsProviderType.ordinal } returns SeedDefinitionsProviderType.REMOTE.ordinal
    mockVoidReturningFunctions()
  }

  private fun mockVoidReturningFunctions() {
    justRun { sourceService.writeConnectorMetadata(any(), any(), any()) }
    justRun { sourceService.updateStandardSourceDefinition(any()) }
    justRun { destinationService.writeConnectorMetadata(any(), any(), any()) }
    justRun { destinationService.updateStandardDestinationDefinition(any()) }
    every { metricClient.count(metric = any(), value = any(), attributes = anyVararg<MetricAttribute>()) } returns mockk<Counter>()
    justRun { supportStateUpdater.updateSupportStates() }
  }

  @Throws(IOException::class)
  private fun mockSeedInitialDefinitions() {
    val seededDefinitionsAndDefaultVersions: MutableMap<UUID, ActorDefinitionVersion> = HashMap()
    seededDefinitionsAndDefaultVersions[POSTGRES_ID] =
      ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES)
    seededDefinitionsAndDefaultVersions[S3_ID] =
      ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3)
    every { actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap() } returns seededDefinitionsAndDefaultVersions
  }

  @Throws(IOException::class)
  private fun verifyActorDefinitionServiceInteractions() {
    verify { actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap() }
    verify { actorDefinitionService.getActorDefinitionIdsInUse() }
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `a new connector should always be written`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3)

    applyDefinitionsHelper.apply(updateAll, reImport)
    verifyActorDefinitionServiceInteractions()

    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3),
      )
    }
    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION),
        )
      }
    }
    verify { supportStateUpdater.updateSupportStates() }

    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `a connector with release candidate should write RC ADVS and ConnectorRollout`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    mockSeedInitialDefinitions()

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_WITH_RC)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_WITH_RC)
    every { connectorRolloutService.insertConnectorRollout(any()) } returns
      ConnectorRollout(
        id = UUID.randomUUID(),
        workflowRunId = "fake-workflow-run-id",
        actorDefinitionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        hasBreakingChanges = false,
        createdAt = Instant.now().toEpochMilli(),
        updatedAt = Instant.now().toEpochMilli(),
        state = ConnectorEnumRolloutState.INITIALIZED,
        tag = null,
      )
    val fakeAdvId = UUID.randomUUID()
    val fakeInitialAdvId = UUID.randomUUID()
    val insertedAdvSource =
      ConnectorRegistryConverters
        .toActorDefinitionVersion(
          SOURCE_POSTGRES_RC,
        ).withVersionId(fakeAdvId)
    val insertedInitialAdvSource =
      ConnectorRegistryConverters
        .toActorDefinitionVersion(
          SOURCE_POSTGRES_RC,
        ).withVersionId(fakeInitialAdvId)
    val insertedAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeAdvId)
    val insertedInitialAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeInitialAdvId)

    every {
      actorDefinitionService.writeActorDefinitionVersion(any())
    } returns
      insertedAdvSource andThen insertedAdvDestination
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(insertedInitialAdvSource) andThen Optional.of(insertedInitialAdvDestination)
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns emptyList()

    applyDefinitionsHelper.apply(updateAll, reImport)

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC),
      )
    }

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC),
      )
    }
    verify { connectorRolloutService.listConnectorRollouts(any(), any()) }
    val capturedArguments = mutableListOf<ConnectorRollout>()

    verify {
      connectorRolloutService.insertConnectorRollout(capture(capturedArguments))
    }

    assertEquals(2, capturedArguments.size)

    val sourceRollout = capturedArguments[0]
    val destinationRollout = capturedArguments[1]

    assertEquals(ConnectorEnumRolloutState.INITIALIZED, sourceRollout.state)
    assertEquals(ConnectorEnumRolloutState.INITIALIZED, destinationRollout.state)

    assertEquals(insertedAdvSource.versionId, sourceRollout.releaseCandidateVersionId)
    assertEquals(insertedAdvDestination.versionId, destinationRollout.releaseCandidateVersionId)

    assertEquals(insertedAdvSource.actorDefinitionId, sourceRollout.actorDefinitionId)
    assertEquals(insertedAdvDestination.actorDefinitionId, destinationRollout.actorDefinitionId)

    // The destination has no rollout config, we test that the defaults are used
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.maxPercentage
        .toInt(),
      sourceRollout.finalTargetRolloutPct,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.maxPercentage.toInt(), destinationRollout.finalTargetRolloutPct)
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.initialPercentage
        .toInt(),
      sourceRollout.initialRolloutPct,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.initialPercentage.toInt(), destinationRollout.initialRolloutPct)
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.advanceDelayMinutes
        .toInt(),
      sourceRollout.maxStepWaitTimeMins,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.advanceDelayMinutes.toInt(), destinationRollout.maxStepWaitTimeMins)

    assertEquals(false, sourceRollout.hasBreakingChanges)
    assertEquals(false, destinationRollout.hasBreakingChanges)
  }

  @ParameterizedTest
  @MethodSource("invalidInsertStates")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `applyReleaseCandidates should not write ConnectorRollout if a rollout exists in a non-canceled state`(state: ConnectorEnumRolloutState) {
    mockSeedInitialDefinitions()
    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_WITH_RC)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_WITH_RC)

    val fakeAdvId = UUID.randomUUID()
    val fakeInitialAdvId = UUID.randomUUID()
    val insertedAdvSource = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC).withVersionId(fakeAdvId)
    val insertedInitialAdvSource = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC).withVersionId(fakeInitialAdvId)
    val insertedAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeAdvId)
    val insertedInitialAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeInitialAdvId)

    every {
      actorDefinitionService.writeActorDefinitionVersion(any())
    } returns
      insertedAdvSource andThen insertedAdvDestination
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(insertedInitialAdvSource) andThen Optional.of(insertedInitialAdvDestination)
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns
      listOf(
        ConnectorRollout(
          id = UUID.randomUUID(),
          workflowRunId = "fake-workflow-run-id",
          actorDefinitionId = UUID.randomUUID(),
          releaseCandidateVersionId = UUID.randomUUID(),
          initialVersionId = UUID.randomUUID(),
          hasBreakingChanges = false,
          createdAt = Instant.now().toEpochMilli(),
          updatedAt = Instant.now().toEpochMilli(),
          state = state,
          tag = null,
        ),
      )

    val rcSourceDefinitions = listOf(SOURCE_POSTGRES_RC)
    val rcDestinationDefinitions = listOf(DESTINATION_S3_RC)

    applyDefinitionsHelper.applyReleaseCandidates(rcSourceDefinitions)
    applyDefinitionsHelper.applyReleaseCandidates(rcDestinationDefinitions)

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC),
      )
    }

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC),
      )
    }

    verify { connectorRolloutService.listConnectorRollouts(any(), any()) }

    verify(exactly = 0) {
      connectorRolloutService.insertConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("validInsertStates")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `applyReleaseCandidates should write ConnectorRollout if a rollout exists in canceled state`(state: ConnectorEnumRolloutState) {
    mockSeedInitialDefinitions()

    val fakeAdvId = UUID.randomUUID()
    val fakeInitialAdvId = UUID.randomUUID()
    val insertedAdvSource = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC).withVersionId(fakeAdvId)
    val insertedInitialAdvSource = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC).withVersionId(fakeInitialAdvId)
    val insertedAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeAdvId)
    val insertedInitialAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeInitialAdvId)

    every {
      actorDefinitionService.writeActorDefinitionVersion(any())
    } returns
      insertedAdvSource andThen insertedAdvDestination
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(insertedInitialAdvSource) andThen Optional.of(insertedInitialAdvDestination)
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns
      listOf(
        ConnectorRollout(
          id = UUID.randomUUID(),
          workflowRunId = "fake-workflow-run-id",
          actorDefinitionId = UUID.randomUUID(),
          releaseCandidateVersionId = UUID.randomUUID(),
          initialVersionId = UUID.randomUUID(),
          hasBreakingChanges = false,
          createdAt = Instant.now().toEpochMilli(),
          updatedAt = Instant.now().toEpochMilli(),
          state = state,
          tag = null,
        ),
      )
    every { connectorRolloutService.insertConnectorRollout(any()) } returns
      ConnectorRollout(
        id = UUID.randomUUID(),
        workflowRunId = "fake-workflow-run-id",
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        hasBreakingChanges = false,
        createdAt = Instant.now().toEpochMilli(),
        updatedAt = Instant.now().toEpochMilli(),
        state = state,
        tag = null,
      )

    val rcSourceDefinitions = listOf(SOURCE_POSTGRES_RC)
    val rcDestinationDefinitions = listOf(DESTINATION_S3_RC)

    applyDefinitionsHelper.applyReleaseCandidates(rcSourceDefinitions)
    applyDefinitionsHelper.applyReleaseCandidates(rcDestinationDefinitions)

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_RC),
      )
    }

    verify {
      actorDefinitionService.writeActorDefinitionVersion(
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC),
      )
    }

    verify { connectorRolloutService.listConnectorRollouts(any(), any()) }

    val capturedArguments = mutableListOf<ConnectorRollout>()

    verify {
      connectorRolloutService.insertConnectorRollout(capture(capturedArguments))
    }

    assertEquals(2, capturedArguments.size)

    val sourceRollout = capturedArguments[0]
    val destinationRollout = capturedArguments[1]

    assertEquals(ConnectorEnumRolloutState.INITIALIZED, sourceRollout.state)
    assertEquals(ConnectorEnumRolloutState.INITIALIZED, destinationRollout.state)

    assertEquals(insertedAdvSource.versionId, sourceRollout.releaseCandidateVersionId)
    assertEquals(insertedAdvDestination.versionId, destinationRollout.releaseCandidateVersionId)

    assertEquals(insertedAdvSource.actorDefinitionId, sourceRollout.actorDefinitionId)
    assertEquals(insertedAdvDestination.actorDefinitionId, destinationRollout.actorDefinitionId)

    // The destination has no rollout config, we test that the defaults are used
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.maxPercentage
        .toInt(),
      sourceRollout.finalTargetRolloutPct,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.maxPercentage.toInt(), destinationRollout.finalTargetRolloutPct)
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.initialPercentage
        .toInt(),
      sourceRollout.initialRolloutPct,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.initialPercentage.toInt(), destinationRollout.initialRolloutPct)
    assertEquals(
      SOURCE_POSTGRES_RC.releases.rolloutConfiguration.advanceDelayMinutes
        .toInt(),
      sourceRollout.maxStepWaitTimeMins,
    )
    assertEquals(ConnectorRegistryConverters.DEFAULT_ROLLOUT_CONFIGURATION.advanceDelayMinutes.toInt(), destinationRollout.maxStepWaitTimeMins)

    assertEquals(false, sourceRollout.hasBreakingChanges)
    assertEquals(false, destinationRollout.hasBreakingChanges)
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `a connector with malformed release candidate should not raise an error`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    mockSeedInitialDefinitions()

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_WITH_MALFORMED_RC)

    every {
      actorDefinitionService.writeActorDefinitionVersion(any())
    } returns
      ConnectorRegistryConverters.toActorDefinitionVersion(
        SOURCE_POSTGRES_WITH_MALFORMED_RC,
      )

    applyDefinitionsHelper.apply(updateAll, reImport)

    verify(exactly = 0) {
      actorDefinitionService.writeActorDefinitionVersion(any())
    }
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `a connector with release candidate with no initial version does not write connector rollout`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    mockSeedInitialDefinitions()

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_WITH_RC)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_WITH_RC)
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()
    every { connectorRolloutService.insertConnectorRollout(any()) } returns
      ConnectorRollout(
        id = UUID.randomUUID(),
        workflowRunId = "fake-workflow-run-id",
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        hasBreakingChanges = false,
        createdAt = Instant.now().toEpochMilli(),
        updatedAt = Instant.now().toEpochMilli(),
        state = ConnectorEnumRolloutState.INITIALIZED,
        tag = null,
      )

    val fakeAdvId = UUID.randomUUID()

    val insertedAdvSource =
      ConnectorRegistryConverters
        .toActorDefinitionVersion(
          SOURCE_POSTGRES_RC,
        ).withVersionId(fakeAdvId)
    val insertedAdvDestination = ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_RC).withVersionId(fakeAdvId)

    every {
      actorDefinitionService.writeActorDefinitionVersion(any())
    } returns
      insertedAdvSource andThen insertedAdvDestination

    applyDefinitionsHelper.apply(updateAll, reImport)

    verify(exactly = 0) {
      connectorRolloutService.insertConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `an existing connector that is not in use should always be updated`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    mockSeedInitialDefinitions()
    every { actorDefinitionService.getActorDefinitionIdsInUse() } returns emptySet()

    // New definitions come in
    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_2)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_2)

    applyDefinitionsHelper.apply(updateAll, reImport)
    verifyActorDefinitionServiceInteractions()

    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2),
      )
    }
    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION),
        )
      }
    }
    verify { supportStateUpdater.updateSupportStates() }

    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @Test
  fun `reImport should refresh the existing definition`() {
    mockSeedInitialDefinitions()
    every { actorDefinitionService.getActorDefinitionIdsInUse() } returns setOf(POSTGRES_ID, S3_ID)

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_2)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_2)

    val versionDefinition1 =
      ActorDefinitionVersion()
        .withDockerRepository("1")

    every { actorDefinitionVersionResolver.fetchRemoteActorDefinitionVersion(any(), any(), any()) } returns
      Optional.of(versionDefinition1)

    applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true)

    verify {
      sourceService.writeConnectorMetadata(any(), eq(versionDefinition1), any())
    }
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `updateAll should affect whether existing connectors in use have their versions updated`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    mockSeedInitialDefinitions()
    every { actorDefinitionService.getActorDefinitionIdsInUse() } returns setOf(POSTGRES_ID, S3_ID)

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES_2)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3_2)

    applyDefinitionsHelper.apply(updateAll, reImport)
    verifyActorDefinitionServiceInteractions()

    if (updateAll) {
      verify {
        sourceService.writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2),
        )
      }
      verify {
        destinationService.writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2),
        )
      }
      listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach { dockerRepo ->
        verify {
          metricClient.count(
            OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            MetricAttribute("status", "ok"),
            MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED.toString()),
            MetricAttribute("docker_repository", dockerRepo),
            MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION),
          )
        }
      }
    } else if (!reImport) {
      verify { sourceService.updateStandardSourceDefinition(ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2)) }
      verify {
        destinationService.updateStandardDestinationDefinition(
          ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        )
      }
      verify(exactly = 2) {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED.toString()),
        )
      }
    }
    verify { supportStateUpdater.updateSupportStates() }

    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `new definitions that are incompatible with the protocol version range should not be written`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    every { jobPersistence.getCurrentProtocolVersionRange() } returns Optional.of(AirbyteProtocolVersionRange(Version("2.0.0"), Version("3.0.0")))
    val postgresWithOldProtocolVersion = Jsons.clone(SOURCE_POSTGRES).withSpec(ConnectorSpecification().withProtocolVersion("1.0.0"))
    val s3withOldProtocolVersion = Jsons.clone(DESTINATION_S3).withSpec(ConnectorSpecification().withProtocolVersion("1.0.0"))

    every { definitionsProvider.getSourceDefinitions() } returns listOf(postgresWithOldProtocolVersion, SOURCE_POSTGRES_2)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(s3withOldProtocolVersion, DESTINATION_S3_2)

    applyDefinitionsHelper.apply(updateAll, reImport)
    verifyActorDefinitionServiceInteractions()

    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "failed"),
          MetricAttribute("outcome", DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION),
        )
      }
    }

    verify(exactly = 0) {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(postgresWithOldProtocolVersion),
      )
    }
    verify(exactly = 0) {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(s3withOldProtocolVersion),
      )
    }
    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2),
      )
    }
    verify { supportStateUpdater.updateSupportStates() }
    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION),
        )
      }
    }
    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @ParameterizedTest
  @MethodSource("updateScenario")
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun `new definitions that are incompatible with the airbyte version range should not be written`(
    updateAll: Boolean,
    reImport: Boolean,
  ) {
    every {
      airbyteCompatibleConnectorsValidator.validate(POSTGRES_ID.toString(), "0.1.0")
    } returns ConnectorPlatformCompatibilityValidationResult(false, "Postgres source 0.1.0 can't be updated")
    every {
      airbyteCompatibleConnectorsValidator.validate(S3_ID.toString(), "0.1.0")
    } returns ConnectorPlatformCompatibilityValidationResult(false, "S3 Destination 0.1.0 can't be updated")
    every { airbyteCompatibleConnectorsValidator.validate(any(), "0.2.0") } returns ConnectorPlatformCompatibilityValidationResult(true, null)

    every { definitionsProvider.getSourceDefinitions() } returns listOf(SOURCE_POSTGRES, SOURCE_POSTGRES_2)
    every { definitionsProvider.getDestinationDefinitions() } returns listOf(DESTINATION_S3, DESTINATION_S3_2)

    applyDefinitionsHelper.apply(updateAll, reImport)
    verifyActorDefinitionServiceInteractions()

    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "failed"),
          MetricAttribute("outcome", DefinitionProcessingFailureReason.INCOMPATIBLE_AIRBYTE_VERSION.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION),
        )
      }
    }

    verify(exactly = 0) {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES),
      )
    }
    verify(exactly = 0) {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3),
      )
    }
    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2),
      )
    }
    verify { supportStateUpdater.updateSupportStates() }
    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach {
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
          MetricAttribute("docker_repository", it),
          MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION),
        )
      }
    }
    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @Test
  fun `one malformed definition should not be written, but shouldn't stop others from being written`() {
    val malformedRegistrySourceDefinition =
      Jsons.clone(SOURCE_POSTGRES).withDockerImageTag("a-non-semantic-version-for-example")
    assertThrows<RuntimeException> { ConnectorRegistryConverters.toActorDefinitionVersion(malformedRegistrySourceDefinition) }

    val malformedRegistryDestinationDefinition =
      Jsons.clone(DESTINATION_S3).withDockerImageTag("a-non-semantic-version-for-example")
    assertThrows<RuntimeException> {
      ConnectorRegistryConverters.toActorDefinitionVersion(
        malformedRegistryDestinationDefinition,
      )
    }

    val anotherNewSourceDefinition =
      Jsons
        .clone(SOURCE_POSTGRES)
        .withName("new")
        .withDockerRepository("airbyte/source-new")
        .withSourceDefinitionId(UUID.randomUUID())
    val anotherNewDestinationDefinition =
      Jsons
        .clone(DESTINATION_S3)
        .withName("new")
        .withDockerRepository("airbyte/destination-new")
        .withDestinationDefinitionId(UUID.randomUUID())

    every { definitionsProvider.getSourceDefinitions() } returns
      listOf(SOURCE_POSTGRES, malformedRegistrySourceDefinition, anotherNewSourceDefinition)
    every {
      definitionsProvider.getDestinationDefinitions()
    } returns listOf(DESTINATION_S3, malformedRegistryDestinationDefinition, anotherNewDestinationDefinition)

    applyDefinitionsHelper.apply(true)
    verifyActorDefinitionServiceInteractions()
    listOf("airbyte/source-postgres", "airbyte/destination-s3").forEach { dockerRepo ->
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "failed"),
          MetricAttribute("outcome", DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED.toString()),
          MetricAttribute("docker_repository", dockerRepo),
          MetricAttribute("docker_image_tag", "a-non-semantic-version-for-example"),
        )
      }
    }
    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3),
      )
    }
    verify {
      sourceService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(anotherNewSourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(anotherNewSourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(anotherNewSourceDefinition),
      )
    }
    verify {
      destinationService.writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(anotherNewDestinationDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(anotherNewDestinationDefinition),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(anotherNewDestinationDefinition),
      )
    }
    verify { supportStateUpdater.updateSupportStates() }
    listOf("airbyte/source-postgres", "airbyte/destination-s3", "airbyte/source-new", "airbyte/destination-new").forEach { dockerRepo ->
      verify {
        metricClient.count(
          OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
          1,
          MetricAttribute("status", "ok"),
          MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
          MetricAttribute("docker_repository", dockerRepo),
          MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION),
        )
      }
    }

    // The malformed definitions should not have been written.
    confirmVerified(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient)
  }

  @ParameterizedTest
  @MethodSource("updateScenarioWithSeedType")
  fun `should only perform version rollbacks when using remote definitions provider`(
    updateAll: Boolean,
    isInUse: Boolean,
    seedType: SeedDefinitionsProviderType,
  ) {
    every { seedDefinitionsProviderType.ordinal } returns seedType.ordinal

    val currentVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2)
    val newVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES)

    val definitionsInUse = if (isInUse) setOf(currentVersion.actorDefinitionId) else setOf()

    val shouldUpdateVersion =
      applyDefinitionsHelper.getShouldUpdateActorDefinitionDefaultVersion(
        currentVersion,
        newVersion,
        definitionsInUse,
        updateAll,
      )

    if (seedType == SeedDefinitionsProviderType.REMOTE && (!isInUse || updateAll)) {
      assertTrue(shouldUpdateVersion)
    } else {
      assertFalse(shouldUpdateVersion)
    }
  }

  @ParameterizedTest
  @MethodSource("updateScenarioWithSeedType")
  fun `should perform version upgrades regardless of definitions provider`(
    updateAll: Boolean,
    isInUse: Boolean,
    seedType: SeedDefinitionsProviderType,
  ) {
    every { seedDefinitionsProviderType.ordinal } returns seedType.ordinal

    val currentVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES)
    val newVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2)

    val definitionsInUse = if (isInUse) setOf(currentVersion.actorDefinitionId) else setOf()

    val shouldUpdateVersion =
      applyDefinitionsHelper.getShouldUpdateActorDefinitionDefaultVersion(
        currentVersion,
        newVersion,
        definitionsInUse,
        updateAll,
      )

    if (!isInUse || updateAll) {
      assertTrue(shouldUpdateVersion)
    } else {
      assertFalse(shouldUpdateVersion)
    }
  }

  @ParameterizedTest
  @MethodSource("updateScenarioWithSeedType")
  fun `should not try to update the connector version if it is already matching`(
    updateAll: Boolean,
    isInUse: Boolean,
    seedType: SeedDefinitionsProviderType,
  ) {
    every { seedDefinitionsProviderType.ordinal } returns seedType.ordinal

    val currentVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES)
    val newVersion = ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES)

    val definitionsInUse = if (isInUse) setOf(currentVersion.actorDefinitionId) else setOf()

    val shouldUpdateVersion =
      applyDefinitionsHelper.getShouldUpdateActorDefinitionDefaultVersion(
        currentVersion,
        newVersion,
        definitionsInUse,
        updateAll,
      )

    assertFalse(shouldUpdateVersion)
  }

  companion object {
    private const val INITIAL_CONNECTOR_VERSION = "0.1.0"
    private const val UPDATED_CONNECTOR_VERSION = "0.2.0"
    private const val BREAKING_CHANGE_VERSION = "1.0.0"

    private const val PROTOCOL_VERSION = "2.0.0"

    private val POSTGRES_ID: UUID = UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750")
    private val sourceRegistryBreakingChanges: BreakingChanges =
      BreakingChanges().withAdditionalProperty(
        BREAKING_CHANGE_VERSION,
        VersionBreakingChange()
          .withMessage("Sample message")
          .withUpgradeDeadline("2023-07-20")
          .withMigrationDocumentationUrl("https://example.com"),
      )

    private val destinationRegistryBreakingChanges: BreakingChanges =
      BreakingChanges().withAdditionalProperty(
        BREAKING_CHANGE_VERSION,
        VersionBreakingChange()
          .withMessage("Sample message")
          .withUpgradeDeadline("2023-07-20")
          .withDeadlineAction("nothing")
          .withMigrationDocumentationUrl("https://example.com"),
      )

    private val SOURCE_POSTGRES: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(POSTGRES_ID)
        .withName("Postgres")
        .withDockerRepository("airbyte/source-postgres")
        .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))

    private val SOURCE_POSTGRES_RC: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(POSTGRES_ID)
        .withName("Postgres")
        .withDockerRepository("airbyte/source-postgres")
        .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(
          ConnectorReleasesSource().withRolloutConfiguration(
            RolloutConfiguration().withMaxPercentage(100).withInitialPercentage(10).withAdvanceDelayMinutes(10),
          ),
        )

    private val SOURCE_POSTGRES_WITH_RC: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(POSTGRES_ID)
        .withName("Postgres")
        .withDockerRepository("airbyte/source-postgres")
        .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(
          ConnectorReleasesSource().withReleaseCandidates(
            ReleaseCandidatesSource().withAdditionalProperty(UPDATED_CONNECTOR_VERSION, SOURCE_POSTGRES_RC),
          ),
        )

    private val SOURCE_POSTGRES_WITH_MALFORMED_RC: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(POSTGRES_ID)
        .withName("Postgres")
        .withDockerRepository("airbyte/source-postgres")
        .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(
          ConnectorReleasesSource().withReleaseCandidates(
            ReleaseCandidatesSource().withAdditionalProperty(UPDATED_CONNECTOR_VERSION, null),
          ),
        )

    private val SOURCE_POSTGRES_2: ConnectorRegistrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(POSTGRES_ID)
        .withName("Postgres - Updated")
        .withDockerRepository("airbyte/source-postgres")
        .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres/new")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges))

    private val S3_ID: UUID = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362")
    private val DESTINATION_S3: ConnectorRegistryDestinationDefinition =
      ConnectorRegistryDestinationDefinition()
        .withName("S3")
        .withDestinationDefinitionId(S3_ID)
        .withDockerRepository("airbyte/destination-s3")
        .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))

    private val DESTINATION_S3_RC: ConnectorRegistryDestinationDefinition =
      ConnectorRegistryDestinationDefinition()
        .withName("S3")
        .withDestinationDefinitionId(S3_ID)
        .withDockerRepository("airbyte/destination-s3")
        .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3")
        .withSpec(
          ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION),
        )

    private val DESTINATION_S3_WITH_RC: ConnectorRegistryDestinationDefinition =
      ConnectorRegistryDestinationDefinition()
        .withName("S3")
        .withDestinationDefinitionId(S3_ID)
        .withDockerRepository("airbyte/destination-s3")
        .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(
          ConnectorReleasesDestination().withReleaseCandidates(
            ReleaseCandidatesDestination().withAdditionalProperty(UPDATED_CONNECTOR_VERSION, DESTINATION_S3_RC),
          ),
        )

    private val DESTINATION_S3_2: ConnectorRegistryDestinationDefinition =
      ConnectorRegistryDestinationDefinition()
        .withName("S3 - Updated")
        .withDestinationDefinitionId(S3_ID)
        .withDockerRepository("airbyte/destination-s3")
        .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
        .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3/new")
        .withSpec(ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
        .withReleases(ConnectorReleasesDestination().withBreakingChanges(destinationRegistryBreakingChanges))

    @JvmStatic
    fun updateScenario(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, false),
        Arguments.of(false, true),
      )

    @JvmStatic
    fun updateScenarioWithSeedType(): Stream<Arguments> =
      Stream.of(
        Arguments.of(true, true, SeedDefinitionsProviderType.REMOTE),
        Arguments.of(true, false, SeedDefinitionsProviderType.REMOTE),
        Arguments.of(false, false, SeedDefinitionsProviderType.REMOTE),
        Arguments.of(false, true, SeedDefinitionsProviderType.REMOTE),
        Arguments.of(true, true, SeedDefinitionsProviderType.LOCAL),
        Arguments.of(true, false, SeedDefinitionsProviderType.LOCAL),
        Arguments.of(false, false, SeedDefinitionsProviderType.LOCAL),
        Arguments.of(false, true, SeedDefinitionsProviderType.LOCAL),
      )

    @JvmStatic
    fun validInsertStates() = listOf(ConnectorEnumRolloutState.CANCELED)

    @JvmStatic
    fun invalidInsertStates() = ConnectorEnumRolloutState.entries.filter { it != ConnectorEnumRolloutState.CANCELED }
  }
}
