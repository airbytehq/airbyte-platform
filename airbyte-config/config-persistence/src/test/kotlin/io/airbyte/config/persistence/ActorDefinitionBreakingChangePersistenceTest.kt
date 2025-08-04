/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

internal class ActorDefinitionBreakingChangePersistenceTest : BaseConfigDatabaseTest() {
  fun createActorDefVersion(actorDefinitionId: UUID?): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withActorDefinitionId(actorDefinitionId)
      .withDockerImageTag("1.0.0")
      .withDockerRepository("repo")
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withSpec(ConnectorSpecification().withProtocolVersion("0.1.0"))

  fun createActorDefVersion(
    actorDefinitionId: UUID?,
    dockerImageTag: String?,
  ): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withActorDefinitionId(actorDefinitionId)
      .withDockerImageTag(dockerImageTag)
      .withDockerRepository("repo")
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withSpec(ConnectorSpecification().withProtocolVersion("0.1.0"))

  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService

  @BeforeEach
  @Throws(SQLException::class, JsonValidationException::class, IOException::class)
  fun setup() {
    truncateAllTables()

    val featureFlagClient: FeatureFlagClient = Mockito.mock(TestClient::class.java)
    val secretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)

    val connectionService = Mockito.mock(ConnectionService::class.java)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val connectionTimelineEventService = Mockito.mock(ConnectionTimelineEventService::class.java)
    val metricClient = Mockito.mock(MetricClient::class.java)
    val actorPaginationServiceHelper = Mockito.mock(ActorServicePaginationHelper::class.java)
    actorDefinitionService = Mockito.spy(ActorDefinitionServiceJooqImpl(database!!))

    sourceService =
      Mockito.spy(
        SourceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretPersistenceConfigService,
          connectionService,
          ActorDefinitionVersionUpdater(
            featureFlagClient,
            connectionService,
            actorDefinitionService,
            scopedConfigurationService,
            connectionTimelineEventService,
          ),
          metricClient,
          actorPaginationServiceHelper,
        ),
      )
    destinationService =
      Mockito.spy(
        DestinationServiceJooqImpl(
          database!!,
          featureFlagClient,
          connectionService,
          ActorDefinitionVersionUpdater(
            featureFlagClient,
            connectionService,
            actorDefinitionService,
            scopedConfigurationService,
            connectionTimelineEventService,
          ),
          metricClient,
          actorPaginationServiceHelper,
        ),
      )

    sourceService.writeConnectorMetadata(
      SOURCE_DEFINITION,
      createActorDefVersion(SOURCE_DEFINITION.sourceDefinitionId!!),
      listOf(BREAKING_CHANGE, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4),
    )
    destinationService.writeConnectorMetadata(
      DESTINATION_DEFINITION,
      createActorDefVersion(DESTINATION_DEFINITION.destinationDefinitionId!!),
      listOf(
        OTHER_CONNECTOR_BREAKING_CHANGE,
      ),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testGetBreakingChanges() {
    val breakingChangesForDef1: List<ActorDefinitionBreakingChange> =
      actorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1)
    Assertions.assertEquals(4, breakingChangesForDef1.size)
    Assertions.assertEquals(BREAKING_CHANGE, breakingChangesForDef1.get(0))

    val breakingChangesForDef2: List<ActorDefinitionBreakingChange> =
      actorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_2)
    Assertions.assertEquals(1, breakingChangesForDef2.size)
    Assertions.assertEquals(OTHER_CONNECTOR_BREAKING_CHANGE, breakingChangesForDef2.get(0))
  }

  @Test
  @Throws(IOException::class)
  fun testUpdateActorDefinitionBreakingChange() {
    // Update breaking change
    val updatedBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(BREAKING_CHANGE.actorDefinitionId!!)
        .withVersion(BREAKING_CHANGE.version!!)
        .withMessage("Updated message")
        .withUpgradeDeadline("2025-12-12") // Updated date
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#updated-miration-url")
        .withScopedImpact(
          listOf(
            BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(
              listOf("stream3"),
            ),
          ),
        )
    sourceService.writeConnectorMetadata(
      SOURCE_DEFINITION,
      createActorDefVersion(SOURCE_DEFINITION.sourceDefinitionId!!),
      listOf(updatedBreakingChange, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4),
    )

    // Check updated breaking change
    val breakingChanges: List<ActorDefinitionBreakingChange> =
      actorDefinitionService.listBreakingChangesForActorDefinition(
        ACTOR_DEFINITION_ID_1,
      )
    Assertions.assertEquals(4, breakingChanges.size)
    Assertions.assertEquals(updatedBreakingChange, breakingChanges.get(0))
  }

  @Test
  @Throws(IOException::class)
  fun testListBreakingChanges() {
    val expectedAllBreakingChanges =
      listOf(
        BREAKING_CHANGE,
        BREAKING_CHANGE_2,
        BREAKING_CHANGE_3,
        BREAKING_CHANGE_4,
        OTHER_CONNECTOR_BREAKING_CHANGE,
      )
    org.assertj.core.api.Assertions.assertThat<ActorDefinitionBreakingChange?>(expectedAllBreakingChanges).containsExactlyInAnyOrderElementsOf(
      actorDefinitionService.listBreakingChanges(),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testListBreakingChangesForVersion() {
    val adv400 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "4.0.0")
    sourceService.writeConnectorMetadata(SOURCE_DEFINITION, adv400, emptyList())

    // no breaking changes for latest default
    Assertions.assertEquals(4, actorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1).size)
    Assertions.assertEquals(0, actorDefinitionService.listBreakingChangesForActorDefinitionVersion(adv400).size)

    // should see future breaking changes for 2.0.0
    val adv200 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "2.0.0")
    Assertions.assertEquals(2, actorDefinitionService.listBreakingChangesForActorDefinitionVersion(adv200).size)
    Assertions.assertEquals(
      listOf(BREAKING_CHANGE_3, BREAKING_CHANGE_4),
      actorDefinitionService.listBreakingChangesForActorDefinitionVersion(adv200),
    )

    // move back default version for Actor Definition to 3.0.0, should stop seeing "rolled back"
    // breaking changes
    val adv300 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "3.0.0")
    sourceService.writeConnectorMetadata(SOURCE_DEFINITION, adv300, emptyList())
    Assertions.assertEquals(1, actorDefinitionService.listBreakingChangesForActorDefinitionVersion(adv200).size)
    Assertions.assertEquals(
      listOf(BREAKING_CHANGE_3),
      actorDefinitionService.listBreakingChangesForActorDefinitionVersion(adv200),
    )
  }

  companion object {
    private val ACTOR_DEFINITION_ID_1: UUID = UUID.randomUUID()
    private val ACTOR_DEFINITION_ID_2: UUID = UUID.randomUUID()

    private val SOURCE_DEFINITION: StandardSourceDefinition =
      StandardSourceDefinition()
        .withName("Test Source")
        .withSourceDefinitionId(ACTOR_DEFINITION_ID_1)

    private val DESTINATION_DEFINITION: StandardDestinationDefinition =
      StandardDestinationDefinition()
        .withName("Test Destination")
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID_2)

    private val BREAKING_CHANGE_SCOPE: BreakingChangeScope =
      BreakingChangeScope()
        .withScopeType(BreakingChangeScope.ScopeType.STREAM)
        .withImpactedScopes(listOf("stream1", "stream2"))

    private val BREAKING_CHANGE: ActorDefinitionBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
        .withVersion(Version("1.0.0"))
        .withMessage("This is an older breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21")
        .withScopedImpact(listOf(BREAKING_CHANGE_SCOPE))
    private val BREAKING_CHANGE_2: ActorDefinitionBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
        .withVersion(Version("2.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#2.0.0")
        .withUpgradeDeadline("2025-02-21")
    private val BREAKING_CHANGE_3: ActorDefinitionBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
        .withVersion(Version("3.0.0"))
        .withMessage("This is another breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#3.0.0")
        .withUpgradeDeadline("2025-03-21")
    private val BREAKING_CHANGE_4: ActorDefinitionBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
        .withVersion(Version("4.0.0"))
        .withMessage("This is some future breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#4.0.0")
        .withUpgradeDeadline("2025-03-21")
    private val OTHER_CONNECTOR_BREAKING_CHANGE: ActorDefinitionBreakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_2)
        .withVersion(Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration-2#1.0.0")
        .withUpgradeDeadline("2025-01-21")
  }
}
