/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.db.jdbc.JdbcUtils
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.UUID

internal class ConfigurationUpdateTest {
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var configurationUpdate: ConfigurationUpdate
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService

  @BeforeEach
  fun setup() {
    secretsProcessor = Mockito.mock(JsonSecretsProcessor::class.java)
    actorDefinitionVersionHelper = Mockito.mock(ActorDefinitionVersionHelper::class.java)
    sourceService = Mockito.mock(SourceService::class.java)
    destinationService = Mockito.mock(DestinationService::class.java)

    configurationUpdate =
      ConfigurationUpdate(
        actorDefinitionVersionHelper,
        sourceService,
        destinationService,
      )
  }

  @Test
  fun testSourceUpdate() {
    Mockito.`when`(sourceService.getSourceConnection(UUID1)).thenReturn(ORIGINAL_SOURCE_CONNECTION)
    Mockito.`when`(sourceService.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION)
    Mockito.`when`(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, WORKSPACE_ID, UUID1)).thenReturn(
      DEFINITION_VERSION,
    )
    Mockito.`when`(secretsProcessor.copySecrets(ORIGINAL_CONFIGURATION, NEW_CONFIGURATION, SPEC)).thenReturn(NEW_CONFIGURATION)

    val actual = configurationUpdate.source(UUID1, ORIGINAL_SOURCE_CONNECTION.getName(), NEW_CONFIGURATION)

    Assertions.assertEquals(NEW_SOURCE_CONNECTION, actual)
    Mockito.verify<ActorDefinitionVersionHelper?>(actorDefinitionVersionHelper).getSourceVersion(SOURCE_DEFINITION, WORKSPACE_ID, UUID1)
  }

  @Test
  fun testDestinationUpdate() {
    Mockito.`when`(destinationService.getDestinationConnection(UUID1)).thenReturn(ORIGINAL_DESTINATION_CONNECTION)
    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(UUID2))
      .thenReturn(DESTINATION_DEFINITION)
    Mockito
      .`when`(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, WORKSPACE_ID, UUID1))
      .thenReturn(
        DEFINITION_VERSION,
      )
    Mockito.`when`(secretsProcessor.copySecrets(ORIGINAL_CONFIGURATION, NEW_CONFIGURATION, SPEC)).thenReturn(NEW_CONFIGURATION)

    val actual = configurationUpdate.destination(UUID1, ORIGINAL_DESTINATION_CONNECTION.getName(), NEW_CONFIGURATION)

    Assertions.assertEquals(NEW_DESTINATION_CONNECTION, actual)
    Mockito.verify(actorDefinitionVersionHelper).getDestinationVersion(DESTINATION_DEFINITION, WORKSPACE_ID, UUID1)
  }

  @Test
  fun testPartialUpdateSourceNoUpdate() {
    Mockito.`when`(sourceService.getSourceConnection(UUID1)).thenReturn(clone(ORIGINAL_SOURCE_CONNECTION))
    Mockito.`when`(sourceService.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION)

    // Test updating nothing
    val noUpdate = configurationUpdate.partialSource(UUID1, null, null)
    Assertions.assertEquals(ORIGINAL_SOURCE_CONNECTION, noUpdate)
  }

  @Test
  fun testPartialUpdateSourceName() {
    Mockito.`when`(sourceService.getSourceConnection(UUID1)).thenReturn(clone(ORIGINAL_SOURCE_CONNECTION))
    Mockito.`when`(sourceService.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION)

    // Test only giving a name
    val updatedName = configurationUpdate.partialSource(UUID1, "TEST", null)
    Assertions.assertEquals(clone(ORIGINAL_SOURCE_CONNECTION).withName("TEST"), updatedName)
  }

  @Test
  fun testPartialUpdateSourceConfig() {
    Mockito.`when`(sourceService.getSourceConnection(UUID1)).thenReturn(clone(ORIGINAL_SOURCE_CONNECTION))
    Mockito.`when`(sourceService.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION)

    // Test updating only configuration
    val updatedConfiguration = configurationUpdate.partialSource(UUID1, null, NEW_CONFIGURATION)
    Assertions.assertEquals(NEW_SOURCE_CONNECTION, updatedConfiguration)
  }

  @Test
  fun testPartialUpdateSourcePartialConfig() {
    Mockito.`when`(sourceService.getSourceConnection(UUID1)).thenReturn(
      clone(ORIGINAL_SOURCE_CONNECTION),
    )
    Mockito.`when`(sourceService.getStandardSourceDefinition(UUID2)).thenReturn(
      SOURCE_DEFINITION,
    )

    // Test partial configuration update
    val partialConfig = jsonNode(mapOf(JdbcUtils.PASSWORD_KEY to "123"))
    val expectedConfiguration =
      jsonNode(
        mapOf(
          JdbcUtils.USERNAME_KEY to "airbyte",
          JdbcUtils.PASSWORD_KEY to "123",
        ),
      )
    val partialUpdateConfiguration = configurationUpdate.partialSource(UUID1, null, partialConfig)
    Assertions.assertEquals(clone(NEW_SOURCE_CONNECTION).withConfiguration(expectedConfiguration), partialUpdateConfiguration)
  }

  companion object {
    private const val IMAGE_REPOSITORY = "foo"
    private const val IMAGE_TAG = "bar"
    private val UUID1: UUID = UUID.randomUUID()
    private val UUID2: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val SPEC: JsonNode =
      CatalogHelpers.fieldsToJsonSchema(
        Field.of(JdbcUtils.USERNAME_KEY, JsonSchemaType.STRING),
        Field.of(JdbcUtils.PASSWORD_KEY, JsonSchemaType.STRING),
      )
    private val CONNECTOR_SPECIFICATION: ConnectorSpecification? = ConnectorSpecification().withConnectionSpecification(SPEC)
    private val ORIGINAL_CONFIGURATION =
      jsonNode(
        mapOf(
          JdbcUtils.USERNAME_KEY to "airbyte",
          JdbcUtils.PASSWORD_KEY to "abc",
        ),
      )
    private val NEW_CONFIGURATION =
      jsonNode(
        mapOf(
          JdbcUtils.USERNAME_KEY to "airbyte",
          JdbcUtils.PASSWORD_KEY to "xyz",
        ),
      )
    private val SOURCE_DEFINITION = StandardSourceDefinition()
    private val DEFINITION_VERSION: ActorDefinitionVersion? =
      ActorDefinitionVersion()
        .withDockerRepository(IMAGE_REPOSITORY)
        .withDockerImageTag(IMAGE_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
    private val ORIGINAL_SOURCE_CONNECTION: SourceConnection =
      SourceConnection()
        .withSourceId(UUID1)
        .withSourceDefinitionId(UUID2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(ORIGINAL_CONFIGURATION)
    private val NEW_SOURCE_CONNECTION: SourceConnection =
      SourceConnection()
        .withSourceId(UUID1)
        .withSourceDefinitionId(UUID2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(NEW_CONFIGURATION)
    private val DESTINATION_DEFINITION = StandardDestinationDefinition()
    private val ORIGINAL_DESTINATION_CONNECTION: DestinationConnection =
      DestinationConnection()
        .withDestinationId(UUID1)
        .withDestinationDefinitionId(UUID2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(ORIGINAL_CONFIGURATION)
    private val NEW_DESTINATION_CONNECTION: DestinationConnection =
      DestinationConnection()
        .withDestinationId(UUID1)
        .withDestinationDefinitionId(UUID2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(NEW_CONFIGURATION)
  }
}
