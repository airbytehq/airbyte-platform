/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteConfig
import io.airbyte.config.ConfigSchema
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Notification
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncState
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.Field
import io.airbyte.protocol.models.v0.SyncMode
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import java.net.URI
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

object SetupForNormalizedTablesTest {
  private val WORKSPACE_ID: UUID = UUID.randomUUID()
  private val WORKSPACE_CUSTOMER_ID: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_2: UUID = UUID.randomUUID()
  private val SOURCE_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_ID_2: UUID = UUID.randomUUID()
  private val OPERATION_ID_1: UUID = UUID.randomUUID()
  private val OPERATION_ID_2: UUID = UUID.randomUUID()
  private val CONNECTION_ID_1: UUID = UUID.randomUUID()
  private val CONNECTION_ID_2: UUID = UUID.randomUUID()
  private val CONNECTION_ID_3: UUID = UUID.randomUUID()
  private val CONNECTION_ID_4: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_2: UUID = UUID.randomUUID()
  private val AIRBYTE_CONFIGS = DSL.table("airbyte_configs")
  private val CONFIG_ID = DSL.field("config_id", String::class.java)
  private val CONFIG_TYPE = DSL.field("config_type", String::class.java)
  private val CONFIG_BLOB = DSL.field("config_blob", JSONB::class.java)
  private val CREATED_AT = DSL.field("created_at", OffsetDateTime::class.java)
  private val UPDATED_AT = DSL.field("updated_at", OffsetDateTime::class.java)
  private val NOW: Instant = Instant.parse("2021-12-15T20:30:40.00Z")
  private const val CONNECTION_SPEC = """'{"name":"John", "age":30, "car":null}'"""

  fun setup(context: DSLContext) {
    createConfigInOldTable(context, standardWorkspace(), ConfigSchema.STANDARD_WORKSPACE)

    standardSourceDefinitions().forEach { createConfigInOldTable(context, it, ConfigSchema.STANDARD_SOURCE_DEFINITION) }
    standardDestinationDefinitions().forEach { createConfigInOldTable(context, it, ConfigSchema.STANDARD_DESTINATION_DEFINITION) }
    sourceConnections().forEach { createConfigInOldTable(context, it, ConfigSchema.SOURCE_CONNECTION) }
    destinationConnections().forEach { createConfigInOldTable(context, it, ConfigSchema.DESTINATION_CONNECTION) }
    sourceOauthParameters().forEach { createConfigInOldTable(context, it, ConfigSchema.SOURCE_OAUTH_PARAM) }
    destinationOauthParameters().forEach { createConfigInOldTable(context, it, ConfigSchema.DESTINATION_OAUTH_PARAM) }
    standardSyncOperations().forEach { createConfigInOldTable(context, it, ConfigSchema.STANDARD_SYNC_OPERATION) }
    standardSyncs().forEach { createConfigInOldTable(context, it, ConfigSchema.STANDARD_SYNC) }
    standardSyncStates().forEach { createConfigInOldTable(context, it, ConfigSchema.STANDARD_SYNC_STATE) }
  }

  private fun <T> createConfigInOldTable(
    context: DSLContext,
    config: T,
    configType: AirbyteConfig,
  ) {
    insertConfigRecord(
      context,
      configType.name(),
      Jsons.jsonNode(config),
      configType.idFieldName,
    )
  }

  private fun insertConfigRecord(
    context: DSLContext,
    configType: String,
    configJson: JsonNode,
    idFieldName: String?,
  ) {
    val configId = if (idFieldName == null) UUID.randomUUID().toString() else configJson[idFieldName].asText()
    context
      .insertInto(AIRBYTE_CONFIGS)
      .set(CONFIG_ID, configId)
      .set(CONFIG_TYPE, configType)
      .set(CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(configJson)))
      .set(CREATED_AT, OffsetDateTime.ofInstant(NOW, ZoneOffset.UTC))
      .set(UPDATED_AT, OffsetDateTime.ofInstant(NOW, ZoneOffset.UTC))
      .onConflict(CONFIG_TYPE, CONFIG_ID)
      .doNothing()
      .execute()
  }

  fun standardWorkspace(): StandardWorkspace {
    val notification =
      Notification()
        .withNotificationType(Notification.NotificationType.SLACK)
        .withSendOnFailure(true)
        .withSendOnSuccess(true)
        .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("webhook-url"))
    return StandardWorkspace()
      .withWorkspaceId(WORKSPACE_ID)
      .withCustomerId(WORKSPACE_CUSTOMER_ID)
      .withName("test-workspace")
      .withSlug("random-string")
      .withEmail("abc@xyz.com")
      .withInitialSetupComplete(true)
      .withAnonymousDataCollection(true)
      .withNews(true)
      .withSecurityUpdates(true)
      .withDisplaySetupWizard(true)
      .withTombstone(false)
      .withNotifications(listOf(notification))
      .withFirstCompletedSync(true)
      .withFeedbackDone(true)
  }

  fun standardSourceDefinitions(): List<V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardSourceDefinition> {
    val connectorSpecification = connectorSpecification()
    val standardSourceDefinition1 =
      V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardSourceDefinition(
        sourceDefinitionId = SOURCE_DEFINITION_ID_1,
        name = "random-source-1",
        dockerRepository = "repository-1",
        dockerImageTag = "tag-1",
        documentationUrl = "documentation-url-1",
        icon = "icon-1",
        sourceType = V0_32_8_001__AirbyteConfigDatabaseDenormalization.SourceType.api,
        spec = connectorSpecification,
      )
    val standardSourceDefinition2 =
      V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardSourceDefinition(
        sourceDefinitionId = SOURCE_DEFINITION_ID_2,
        name = "random-source-2",
        dockerRepository = "repository-2",
        dockerImageTag = "tag-1",
        documentationUrl = "documentation-url-2",
        icon = "icon-2",
        sourceType = V0_32_8_001__AirbyteConfigDatabaseDenormalization.SourceType.database,
        spec = null,
      )
    return listOf(standardSourceDefinition1, standardSourceDefinition2)
  }

  private fun connectorSpecification(): ConnectorSpecification =
    ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(CONNECTION_SPEC))
      .withDocumentationUrl(URI.create("whatever"))
      .withAdvancedAuth(AdvancedAuth().withAuthFlowType(AdvancedAuth.AuthFlowType.OAUTH_2_0))
      .withChangelogUrl(URI.create("whatever"))
      .withSupportedDestinationSyncModes(
        listOf(
          DestinationSyncMode.APPEND,
          DestinationSyncMode.OVERWRITE,
          DestinationSyncMode.APPEND_DEDUP,
        ),
      ).withSupportsDBT(true)
      .withSupportsIncremental(true)
      .withSupportsNormalization(true)

  fun standardDestinationDefinitions(): List<V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardDestinationDefinition> {
    val connectorSpecification = connectorSpecification()
    val standardDestinationDefinition1 =
      V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardDestinationDefinition(
        destinationDefinitionId = DESTINATION_DEFINITION_ID_1,
        name = "random-destination-1",
        dockerRepository = "repository-3",
        dockerImageTag = "tag-3",
        documentationUrl = "documentation-url-3",
        icon = "icon-3",
        spec = connectorSpecification,
      )
    val standardDestinationDefinition2 =
      V0_32_8_001__AirbyteConfigDatabaseDenormalization.StandardDestinationDefinition(
        destinationDefinitionId = DESTINATION_DEFINITION_ID_2,
        name = "random-destination-2",
        dockerRepository = "repository-4",
        dockerImageTag = "tag-4",
        documentationUrl = "documentation-url-4",
        icon = "icon-4",
        spec = connectorSpecification,
      )
    return listOf(standardDestinationDefinition1, standardDestinationDefinition2)
  }

  fun sourceConnections(): List<SourceConnection> {
    val sourceConnection1 =
      SourceConnection()
        .withName("source-1")
        .withTombstone(false)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withSourceId(SOURCE_ID_1)
    val sourceConnection2 =
      SourceConnection()
        .withName("source-2")
        .withTombstone(false)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withSourceId(SOURCE_ID_2)
    return listOf(sourceConnection1, sourceConnection2)
  }

  fun destinationConnections(): List<DestinationConnection> {
    val destinationConnection1 =
      DestinationConnection()
        .withName("destination-1")
        .withTombstone(false)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_1)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withDestinationId(DESTINATION_ID_1)
    val destinationConnection2 =
      DestinationConnection()
        .withName("destination-2")
        .withTombstone(false)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withDestinationId(DESTINATION_ID_2)
    return listOf(destinationConnection1, destinationConnection2)
  }

  fun sourceOauthParameters(): List<SourceOAuthParameter?> {
    val sourceOAuthParameter1 =
      SourceOAuthParameter()
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withWorkspaceId(null)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_1)
    val sourceOAuthParameter2 =
      SourceOAuthParameter()
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_2)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_2)
    return listOf(sourceOAuthParameter1, sourceOAuthParameter2)
  }

  fun destinationOauthParameters(): List<DestinationOAuthParameter?> {
    val destinationOAuthParameter1 =
      DestinationOAuthParameter()
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withWorkspaceId(null)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_1)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_1)
    val destinationOAuthParameter2 =
      DestinationOAuthParameter()
        .withConfiguration(Jsons.jsonNode(CONNECTION_SPEC))
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_2)
    return listOf(destinationOAuthParameter1, destinationOAuthParameter2)
  }

  fun standardSyncOperations(): List<StandardSyncOperation> {
    val standardSyncOperation1 =
      StandardSyncOperation()
        .withName("operation-1")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_1)
        .withWorkspaceId(WORKSPACE_ID)
        .withOperatorType(StandardSyncOperation.OperatorType.NORMALIZATION)
        .withOperatorWebhook(OperatorWebhook())
    val standardSyncOperation2 =
      StandardSyncOperation()
        .withName("operation-1")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_2)
        .withWorkspaceId(WORKSPACE_ID)
        .withOperatorType(StandardSyncOperation.OperatorType.NORMALIZATION)
        .withOperatorWebhook(OperatorWebhook())
    return listOf(standardSyncOperation1, standardSyncOperation2)
  }

  fun standardSyncs(): List<StandardSync> {
    val resourceRequirements =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("1")
        .withMemoryRequest("1")
        .withMemoryLimit("1")
    val schedule = Schedule().withTimeUnit(Schedule.TimeUnit.DAYS).withUnits(1L)
    val standardSync1 =
      StandardSync()
        .withOperationIds(listOf(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_1)
        .withSourceId(SOURCE_ID_1)
        .withDestinationId(DESTINATION_ID_1)
        .withCatalog(
          Jsons.convertValue(
            configuredCatalog,
            io.airbyte.config.ConfiguredAirbyteCatalog::class.java,
          ),
        ).withName("standard-sync-1")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)

    val standardSync2 =
      StandardSync()
        .withOperationIds(listOf(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_2)
        .withSourceId(SOURCE_ID_1)
        .withDestinationId(DESTINATION_ID_2)
        .withCatalog(
          Jsons.convertValue(
            configuredCatalog,
            io.airbyte.config.ConfiguredAirbyteCatalog::class.java,
          ),
        ).withName("standard-sync-2")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)

    val standardSync3 =
      StandardSync()
        .withOperationIds(listOf(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_3)
        .withSourceId(SOURCE_ID_2)
        .withDestinationId(DESTINATION_ID_1)
        .withCatalog(
          Jsons.convertValue(
            configuredCatalog,
            io.airbyte.config.ConfiguredAirbyteCatalog::class.java,
          ),
        ).withName("standard-sync-3")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.DESTINATION)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)

    val standardSync4 =
      StandardSync()
        .withOperationIds(listOf(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_4)
        .withSourceId(SOURCE_ID_2)
        .withDestinationId(DESTINATION_ID_2)
        .withCatalog(
          Jsons.convertValue(
            configuredCatalog,
            io.airbyte.config.ConfiguredAirbyteCatalog::class.java,
          ),
        ).withName("standard-sync-4")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.INACTIVE)
        .withSchedule(schedule)

    return listOf(standardSync1, standardSync2, standardSync3, standardSync4)
  }

  private val configuredCatalog: ConfiguredAirbyteCatalog
    get() {
      val catalog =
        AirbyteCatalog().withStreams(
          listOf(
            CatalogHelpers
              .createAirbyteStream(
                "models",
                "models_schema",
                Field.of("id", JsonSchemaType.NUMBER),
                Field.of("make_id", JsonSchemaType.NUMBER),
                Field.of("model", JsonSchemaType.STRING),
              ).withSupportedSyncModes(
                listOf(
                  SyncMode.FULL_REFRESH,
                  SyncMode.INCREMENTAL,
                ),
              ).withSourceDefinedPrimaryKey(
                listOf(
                  listOf("id"),
                ),
              ),
          ),
        )
      return CatalogHelpers.toDefaultConfiguredCatalog(catalog)
    }

  fun standardSyncStates(): List<StandardSyncState> {
    val standardSyncState1 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_1)
        .withState(State().withState(Jsons.jsonNode(CONNECTION_SPEC)))
    val standardSyncState2 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_2)
        .withState(State().withState(Jsons.jsonNode(CONNECTION_SPEC)))
    val standardSyncState3 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_3)
        .withState(State().withState(Jsons.jsonNode(CONNECTION_SPEC)))
    val standardSyncState4 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_4)
        .withState(State().withState(Jsons.jsonNode(CONNECTION_SPEC)))
    return listOf(standardSyncState1, standardSyncState2, standardSyncState3, standardSyncState4)
  }

  fun now(): Instant = NOW
}
