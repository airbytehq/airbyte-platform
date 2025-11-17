/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ActorDefinitionServiceJooqImplTest : BaseConfigDatabaseTest() {
  private lateinit var jooqTestDbSetupHelper: JooqTestDbSetupHelper
  private lateinit var sourceService: SourceService
  private lateinit var actorDefinitionService: ActorDefinitionServiceJooqImpl

  @BeforeEach
  fun setUp() {
    this.actorDefinitionService = ActorDefinitionServiceJooqImpl(database)

    val featureFlagClient = mockk<TestClient>()
    every {
      featureFlagClient.stringVariation(
        HeartbeatMaxSecondsBetweenMessages,
        any(),
      )
    } returns "3600"

    val metricClient = mockk<MetricClient>()
    val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>()
    val connectionService = mockk<ConnectionService>()
    val scopedConfigurationService = mockk<ScopedConfigurationService>()
    val connectionTimelineEventService = mockk<ConnectionTimelineEventService>()
    val actorPaginationServiceHelper = mockk<ActorServicePaginationHelper>()
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService,
      )
    this.sourceService =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )

    jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()
  }

  @Test
  fun updateActorDefinitionDefaultVersionId() {
    val actorDefinitionId = jooqTestDbSetupHelper.sourceDefinition!!.sourceDefinitionId
    val sourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId)
    val initialSourceDefVersionId = sourceDefinition.defaultVersionId

    val newVersionId = UUID.randomUUID()
    val newVersion =
      clone(jooqTestDbSetupHelper.sourceDefinitionVersion!!)
        .withDockerImageTag("5.0.0")
    newVersion.versionId = newVersionId
    actorDefinitionService.writeActorDefinitionVersion(newVersion)

    actorDefinitionService.updateActorDefinitionDefaultVersionId(actorDefinitionId, newVersion.versionId)

    val updatedSourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId)
    Assertions.assertEquals(updatedSourceDefinition.defaultVersionId, newVersion.versionId)
    Assertions.assertNotEquals(updatedSourceDefinition.defaultVersionId, initialSourceDefVersionId)
  }

  @Test
  fun shouldNotFailOnDuplicateWrite() {
    val version = jooqTestDbSetupHelper.sourceDefinitionVersion!!
    Assertions.assertDoesNotThrow { actorDefinitionService.writeActorDefinitionVersion(version) }
  }

  @Test
  fun getActorDefinitionVersion() {
    val actorDefinitionVersion =
      actorDefinitionService.getActorDefinitionVersion(
        jooqTestDbSetupHelper.sourceDefinition!!.sourceDefinitionId,
        jooqTestDbSetupHelper.sourceDefinitionVersion!!.dockerImageTag,
      )
    Assertions.assertTrue(actorDefinitionVersion.isPresent)
    Assertions.assertEquals(jooqTestDbSetupHelper.sourceDefinitionVersion, actorDefinitionVersion.get())
  }

  @Test
  fun listActorDefinitionVersionsForDefinition() {
    val actorDefinitionVersions =
      actorDefinitionService.listActorDefinitionVersionsForDefinition(
        jooqTestDbSetupHelper.sourceDefinition!!.sourceDefinitionId,
      )
    Assertions.assertEquals(listOf(jooqTestDbSetupHelper.sourceDefinitionVersion), actorDefinitionVersions)
  }
}
