/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Organization
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.Map
import java.util.UUID

internal class ConfigInjectionTest : BaseConfigDatabaseTest() {
  private lateinit var configInjector: ConfigInjector
  private lateinit var sourceDefinition: StandardSourceDefinition
  private lateinit var exampleConfig: JsonNode

  private lateinit var connectorBuilderService: ConnectorBuilderService
  private lateinit var sourceService: SourceService

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    truncateAllTables()
    val featureFlagClient: FeatureFlagClient = Mockito.mock(TestClient::class.java)
    val secretPersistenceConfigService: SecretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)
    val organizationService: OrganizationService = OrganizationServiceJooqImpl(database)
    organizationService.writeOrganization(
      Organization()
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEmail("test@test.com"),
    )
    val dataplaneGroupService: DataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )
    val connectionService: ConnectionService = ConnectionServiceJooqImpl(database)
    val scopedConfigurationService: ScopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    val actorDefinitionService: ActorDefinitionService = ActorDefinitionServiceJooqImpl(database)
    val connectionTimelineEventService: ConnectionTimelineEventService = Mockito.mock(ConnectionTimelineEventService::class.java)
    val metricClient: MetricClient = Mockito.mock(MetricClient::class.java)
    val actorServicePaginationHelper: ActorServicePaginationHelper = Mockito.mock(ActorServicePaginationHelper::class.java)

    connectorBuilderService = ConnectorBuilderServiceJooqImpl(database)
    sourceService =
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
        actorServicePaginationHelper,
      )
    configInjector = ConfigInjector(ConnectorBuilderServiceJooqImpl(database))
    exampleConfig = jsonNode(Map.of(SAMPLE_CONFIG_KEY, 123))
  }

  @Test
  @Throws(IOException::class)
  fun testInject() {
    createBaseObjects()

    val injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals(123f, injected.get(SAMPLE_CONFIG_KEY).longValue().toFloat(), 123f)
    Assertions.assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertFalse(injected.has("c"))
  }

  @Test
  @Throws(IOException::class)
  fun testInjectOverwrite() {
    createBaseObjects()

    (exampleConfig as ObjectNode).set<JsonNode?>("a", LongNode(123))
    (exampleConfig as ObjectNode).remove(SAMPLE_CONFIG_KEY)

    val injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertFalse(injected.has("c"))
  }

  @Test
  @Throws(IOException::class)
  fun testUpdate() {
    createBaseObjects()

    // write an injection object with the same definition id and the same injection path - will update
    // the existing one
    connectorBuilderService.writeActorDefinitionConfigInjectionForPath(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withInjectionPath("a")
        .withJsonToInject(TextNode("abc")),
    )

    val injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals(123f, injected.get(SAMPLE_CONFIG_KEY).longValue().toFloat(), 123f)
    Assertions.assertEquals("abc", injected.get("a").asText())
    Assertions.assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertFalse(injected.has("c"))
  }

  @Test
  @Throws(IOException::class)
  fun testCreate() {
    createBaseObjects()

    // write an injection object with the same definition id and a new injection path - will create a
    // new one and leave the others in place
    connectorBuilderService.writeActorDefinitionConfigInjectionForPath(
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withInjectionPath("c")
        .withJsonToInject(TextNode("thirdInject")),
    )

    val injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId())
    Assertions.assertEquals(123, injected.get(SAMPLE_CONFIG_KEY).longValue())
    Assertions.assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText())
    Assertions.assertEquals("thirdInject", injected.get("c").asText())
  }

  @Throws(IOException::class)
  private fun createBaseObjects() {
    sourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion: ActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())

    createInjection(sourceDefinition, "a")
    createInjection(sourceDefinition, "b")

    // unreachable injection, should not show up
    val otherSourceDefinition: StandardSourceDefinition = createBaseSourceDef()
    val actorDefinitionVersion2: ActorDefinitionVersion = createBaseActorDefVersion(otherSourceDefinition.getSourceDefinitionId())
    sourceService.writeConnectorMetadata(otherSourceDefinition, actorDefinitionVersion2, mutableListOf())
    createInjection(otherSourceDefinition, "c")
  }

  @Throws(IOException::class)
  private fun createInjection(
    definition: StandardSourceDefinition,
    path: String,
  ): ActorDefinitionConfigInjection {
    val injection =
      ActorDefinitionConfigInjection()
        .withActorDefinitionId(definition.getSourceDefinitionId())
        .withInjectionPath(path)
        .withJsonToInject(jsonNode(mapOf(SAMPLE_INJECTED_KEY to path)))

    connectorBuilderService.writeActorDefinitionConfigInjectionForPath(injection)
    return injection
  }

  companion object {
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.randomUUID()
    private const val SAMPLE_CONFIG_KEY = "my_config_key"
    private const val SAMPLE_INJECTED_KEY = "injected_under"

    private fun createBaseSourceDef(): StandardSourceDefinition {
      val id = UUID.randomUUID()

      return StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
    }

    private fun createBaseActorDefVersion(actorDefId: UUID?): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag("1.0.0")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withSpec(ConnectorSpecification().withProtocolVersion("0.1.0"))
  }
}
