/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons.clone
import io.airbyte.data.ConfigNotFoundException
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
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

internal class ActorDefinitionServiceJooqImplTest : BaseConfigDatabaseTest() {
  private lateinit var jooqTestDbSetupHelper: JooqTestDbSetupHelper
  private lateinit var sourceService: SourceService
  private lateinit var actorDefinitionService: ActorDefinitionServiceJooqImpl

  @BeforeEach
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, SQLException::class)
  fun setUp() {
    this.actorDefinitionService = ActorDefinitionServiceJooqImpl(database)

    val featureFlagClient = Mockito.mock(TestClient::class.java)
    Mockito
      .`when`(
        featureFlagClient.stringVariation(
          org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages),
          org.mockito.kotlin.anyOrNull(),
        ),
      ).thenReturn("3600")

    val metricClient = Mockito.mock(MetricClient::class.java)
    val secretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)
    val connectionService = Mockito.mock(ConnectionService::class.java)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineEventService = Mockito.mock(ConnectionTimelineEventService::class.java)
    val actorPaginationServiceHelper = Mockito.mock(ActorServicePaginationHelper::class.java)
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
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
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
  @Throws(JsonValidationException::class, IOException::class)
  fun shouldNotFailOnDuplicateWrite() {
    val version = jooqTestDbSetupHelper.sourceDefinitionVersion!!
    Assertions.assertDoesNotThrow { actorDefinitionService.writeActorDefinitionVersion(version) }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
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
  @Throws(JsonValidationException::class, IOException::class)
  fun listActorDefinitionVersionsForDefinition() {
    val actorDefinitionVersions =
      actorDefinitionService.listActorDefinitionVersionsForDefinition(
        jooqTestDbSetupHelper.sourceDefinition!!.sourceDefinitionId,
      )
    Assertions.assertEquals(listOf(jooqTestDbSetupHelper.sourceDefinitionVersion), actorDefinitionVersions)
  }
}
