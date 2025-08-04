/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.ReleaseStage
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.SupportLevel
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import java.io.IOException
import java.util.Map
import java.util.UUID
import org.mockito.Mockito.`when` as whenever

/**
 * Tests for interacting with the actor_definition_version table.
 */
internal class ActorDefinitionVersionPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var sourceDefinition: StandardSourceDefinition
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceService: SourceService

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    truncateAllTables()

    val featureFlagClient = mock(TestClient::class.java)
    whenever(
      featureFlagClient.stringVariation(org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages), org.mockito.kotlin.any<SourceDefinition>()),
    ).thenReturn("3600")

    val secretPersistenceConfigService = mock(SecretPersistenceConfigService::class.java)

    actorDefinitionService = spy(ActorDefinitionServiceJooqImpl(database!!))
    val connectionService = mock(ConnectionService::class.java)
    val scopedConfigurationService = mock(ScopedConfigurationService::class.java)
    val connectionTimelineEventService = mock(ConnectionTimelineEventService::class.java)
    val metricClient = mock(MetricClient::class.java)
    val actorPaginationServiceHelper = mock(ActorServicePaginationHelper::class.java)

    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService,
      )

    sourceService =
      spy(
        SourceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretPersistenceConfigService,
          connectionService,
          actorDefinitionVersionUpdater,
          metricClient,
          actorPaginationServiceHelper,
        ),
      )

    val defId = UUID.randomUUID()
    val initialADV = initialActorDefinitionVersion(defId)
    sourceDefinition = baseSourceDefinition(defId)

    // Make sure that the source definition exists before we start writing actor definition versions
    sourceService.writeConnectorMetadata(sourceDefinition, initialADV, emptyList())
  }

  @Test
  @Throws(IOException::class)
  fun testWriteActorDefinitionVersion() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val adv: ActorDefinitionVersion = baseActorDefinitionVersion(defId)
    val writtenADV = actorDefinitionService.writeActorDefinitionVersion(adv)

    // All non-ID fields should match (the ID is randomly assigned)
    val expectedADV = adv.withVersionId(writtenADV.getVersionId())

    Assertions.assertEquals(expectedADV, writtenADV)
  }

  @Test
  @Throws(IOException::class)
  fun testGetActorDefinitionVersionByTag() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val adv: ActorDefinitionVersion = baseActorDefinitionVersion(defId)
    val actorDefinitionVersion = actorDefinitionService.writeActorDefinitionVersion(adv)
    val id = actorDefinitionVersion.getVersionId()

    val optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(adv.withVersionId(id), optRetrievedADV.get())
  }

  @Test
  @Throws(IOException::class)
  fun testUpdateActorDefinitionVersion() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val initialADV: ActorDefinitionVersion = baseActorDefinitionVersion(defId)

    // initial insert
    val insertedADV = actorDefinitionService.writeActorDefinitionVersion(clone<ActorDefinitionVersion>(initialADV))
    val id = insertedADV.getVersionId()

    var optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(insertedADV, optRetrievedADV.get())
    Assertions.assertEquals(clone<ActorDefinitionVersion>(initialADV).withVersionId(id), optRetrievedADV.get())

    // update w/o ID
    val advWithNewSpec = clone<ActorDefinitionVersion>(initialADV).withSpec(SPEC_2)
    val updatedADV = actorDefinitionService.writeActorDefinitionVersion(advWithNewSpec)

    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(updatedADV, optRetrievedADV.get())
    Assertions.assertEquals(clone<ActorDefinitionVersion>(advWithNewSpec).withVersionId(id), optRetrievedADV.get())

    // update w/ ID
    val advWithAnotherNewSpecAndId = clone<ActorDefinitionVersion>(updatedADV).withSpec(SPEC_3)
    val updatedADV2 = actorDefinitionService.writeActorDefinitionVersion(advWithAnotherNewSpecAndId)

    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(updatedADV2, optRetrievedADV.get())
    Assertions.assertEquals(advWithAnotherNewSpecAndId, optRetrievedADV.get())
  }

  @Test
  @Throws(IOException::class)
  fun testUpdateActorDefinitionVersionWithMismatchedIdFails() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val initialADV: ActorDefinitionVersion = baseActorDefinitionVersion(defId)

    // initial insert
    val insertedADV = actorDefinitionService.writeActorDefinitionVersion(clone<ActorDefinitionVersion>(initialADV))
    val id = insertedADV.getVersionId()

    var optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(insertedADV, optRetrievedADV.get())
    Assertions.assertEquals(clone<ActorDefinitionVersion>(initialADV).withVersionId(id), optRetrievedADV.get())

    // update same tag w/ different ID throws
    val advWithNewId = clone<ActorDefinitionVersion>(initialADV).withSpec(SPEC_2).withVersionId(UUID.randomUUID())
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable { actorDefinitionService.writeActorDefinitionVersion(advWithNewId) },
    )

    // no change in DB
    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optRetrievedADV.isPresent())
    Assertions.assertEquals(clone<ActorDefinitionVersion>(initialADV).withVersionId(id), optRetrievedADV.get())
  }

  @Test
  @Throws(IOException::class)
  fun testGetForNonExistentTagReturnsEmptyOptional() {
    val defId = sourceDefinition.getSourceDefinitionId()
    Assertions.assertTrue(actorDefinitionService.getActorDefinitionVersion(defId, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testGetActorDefinitionVersionById() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val adv: ActorDefinitionVersion = baseActorDefinitionVersion(defId)
    val actorDefinitionVersion = actorDefinitionService.writeActorDefinitionVersion(adv)
    val id = actorDefinitionVersion.getVersionId()

    val retrievedADV = actorDefinitionService.getActorDefinitionVersion(id)
    Assertions.assertNotNull(retrievedADV)
    Assertions.assertEquals(adv.withVersionId(id), retrievedADV)
  }

  @Test
  fun testGetActorDefinitionVersionByIdNotExistentThrowsConfigNotFound() {
    // Test using the definition id to catch any accidental assignment
    val defId = sourceDefinition.getSourceDefinitionId()

    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable { actorDefinitionService.getActorDefinitionVersion(defId) },
    )
  }

  @Test
  @Throws(IOException::class)
  fun testWriteSourceDefinitionSupportLevelNone() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val adv: ActorDefinitionVersion = baseActorDefinitionVersion(defId).withActorDefinitionId(defId).withSupportLevel(SupportLevel.NONE)

    sourceService.writeConnectorMetadata(sourceDefinition, adv, mutableListOf<ActorDefinitionBreakingChange>())

    val optADVForTag = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG)
    Assertions.assertTrue(optADVForTag.isPresent())
    val advForTag = optADVForTag.get()
    Assertions.assertEquals(advForTag.getSupportLevel(), SupportLevel.NONE)
  }

  @Test
  fun testWriteSourceDefinitionSupportLevelNonNullable() {
    val defId = sourceDefinition.getSourceDefinitionId()

    val adv: ActorDefinitionVersion = baseActorDefinitionVersion(defId).withActorDefinitionId(defId).withSupportLevel(null)

    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable { sourceService.writeConnectorMetadata(sourceDefinition, adv, mutableListOf<ActorDefinitionBreakingChange>()) },
    )
  }

  @Test
  @Throws(IOException::class)
  fun testAlwaysGetWithProtocolVersion() {
    val defId = sourceDefinition.getSourceDefinitionId()

    val allActorDefVersions =
      listOf(
        baseActorDefinitionVersion(defId).withDockerImageTag("5.0.0").withProtocolVersion(null),
        baseActorDefinitionVersion(defId)
          .withDockerImageTag("5.0.1")
          .withProtocolVersion(null)
          .withSpec(ConnectorSpecification().withProtocolVersion("0.3.1")),
        baseActorDefinitionVersion(defId)
          .withDockerImageTag("5.0.2")
          .withProtocolVersion("0.4.0")
          .withSpec(ConnectorSpecification().withProtocolVersion("0.4.1")),
        baseActorDefinitionVersion(defId)
          .withDockerImageTag("5.0.3")
          .withProtocolVersion("0.5.0")
          .withSpec(ConnectorSpecification()),
      )

    val versionIds: MutableList<UUID> = ArrayList()
    for (actorDefVersion in allActorDefVersions) {
      versionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId())
    }

    val actorDefinitionVersions: List<ActorDefinitionVersion> = actorDefinitionService.getActorDefinitionVersions(versionIds)
    val protocolVersions = actorDefinitionVersions.map { it.getProtocolVersion() }
    Assertions.assertEquals(
      listOf(
        AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize(),
        AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize(),
        "0.4.0",
        "0.5.0",
      ),
      protocolVersions,
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListActorDefinitionVersionsForDefinition() {
    val defId = sourceDefinition.getSourceDefinitionId()
    val otherSourceDef =
      StandardSourceDefinition()
        .withName("Some other source")
        .withSourceDefinitionId(UUID.randomUUID())
    val otherActorDefVersion: ActorDefinitionVersion =
      baseActorDefinitionVersion(defId).withActorDefinitionId(otherSourceDef.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(otherSourceDef, otherActorDefVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val otherActorDefVersionId = sourceService.getStandardSourceDefinition(otherSourceDef.getSourceDefinitionId()).getDefaultVersionId()

    val actorDefinitionVersions =
      listOf(
        baseActorDefinitionVersion(defId).withDockerImageTag("1.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("2.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("3.0.0"),
      )

    val expectedVersionIds: MutableList<UUID> = ArrayList()
    for (actorDefVersion in actorDefinitionVersions) {
      expectedVersionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId())
    }

    val defaultVersionId = sourceService.getStandardSourceDefinition(defId).getDefaultVersionId()
    expectedVersionIds.add(defaultVersionId)

    val actorDefinitionVersionsForDefinition: List<ActorDefinitionVersion> =
      actorDefinitionService.listActorDefinitionVersionsForDefinition(defId)
    org.assertj.core.api.Assertions
      .assertThat<UUID?>(expectedVersionIds)
      .containsExactlyInAnyOrderElementsOf(
        actorDefinitionVersionsForDefinition.map { it.getVersionId() },
      )
    Assertions.assertFalse(
      actorDefinitionVersionsForDefinition
        .any { actorDefVersion: ActorDefinitionVersion -> actorDefVersion.getVersionId() == otherActorDefVersionId },
    )
  }

  @ParameterizedTest
  @CsvSource(
    "SUPPORTED, DEPRECATED",
    "SUPPORTED, UNSUPPORTED",
    "DEPRECATED, SUPPORTED",
    "DEPRECATED, UNSUPPORTED",
    "UNSUPPORTED, SUPPORTED",
    "UNSUPPORTED, DEPRECATED",
  )
  @Throws(IOException::class)
  fun testSetActorDefinitionVersionSupportStates(
    initialSupportStateStr: String,
    targetSupportStateStr: String,
  ) {
    val defId = sourceDefinition.getSourceDefinitionId()
    val initialSupportState = ActorDefinitionVersion.SupportState.valueOf(initialSupportStateStr)
    val targetSupportState = ActorDefinitionVersion.SupportState.valueOf(targetSupportStateStr)

    val actorDefinitionVersions =
      listOf(
        baseActorDefinitionVersion(defId).withDockerImageTag("1.0.0").withSupportState(initialSupportState),
        baseActorDefinitionVersion(defId).withDockerImageTag("2.0.0").withSupportState(initialSupportState),
      )

    val versionIds: MutableList<UUID> = ArrayList()
    for (actorDefVersion in actorDefinitionVersions) {
      versionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId())
    }

    actorDefinitionService.setActorDefinitionVersionSupportStates(versionIds, targetSupportState)

    val updatedActorDefinitionVersions: List<ActorDefinitionVersion> = actorDefinitionService.getActorDefinitionVersions(versionIds)
    for (updatedActorDefinitionVersion in updatedActorDefinitionVersions) {
      Assertions.assertEquals(targetSupportState, updatedActorDefinitionVersion.getSupportState())
    }
  }

  companion object {
    private const val SOURCE_NAME = "Test Source"
    private const val DOCKER_REPOSITORY = "airbyte/source-test"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
    private const val UNPERSISTED_DOCKER_IMAGE_TAG = "0.1.1"
    private const val PROTOCOL_VERSION = "1.0.0"
    private val SPEC: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key", "value")))
        .withProtocolVersion(
          PROTOCOL_VERSION,
        )
    private val SPEC_2: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key2", "value2")))
        .withProtocolVersion(
          PROTOCOL_VERSION,
        )
    private val SPEC_3: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key3", "value3")))
        .withProtocolVersion(
          PROTOCOL_VERSION,
        )

    private fun baseSourceDefinition(actorDefinitionId: UUID?): StandardSourceDefinition =
      StandardSourceDefinition()
        .withName(SOURCE_NAME)
        .withSourceDefinitionId(actorDefinitionId)

    private fun initialActorDefinitionVersion(actorDefinitionId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag("0.0.0")
        .withDockerRepository("overwrite me")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withLanguage("manifest-only")
        .withSpec(ConnectorSpecification().withAdditionalProperty("overwrite", "me").withProtocolVersion("0.0.0"))

    private fun baseActorDefinitionVersion(actorDefinitionId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl("https://airbyte.io/docs/")
        .withReleaseStage(ReleaseStage.BETA)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withReleaseDate("2021-01-21")
        .withSuggestedStreams(SuggestedStreams().withStreams(mutableListOf<String?>("users")))
        .withProtocolVersion("0.1.0")
        .withAllowedHosts(AllowedHosts().withHosts(mutableListOf<String?>("https://airbyte.com")))
  }
}
