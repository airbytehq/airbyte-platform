/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.cronutils.utils.VisibleForTesting
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithFullDetails
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.impls.data.mappers.objectMapper
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.server.apis.publicapi.services.JobService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.airbyte.validation.json.JsonMergingHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class PartialUserConfigHandler(
  private val partialUserConfigService: PartialUserConfigService,
  private val configTemplateService: ConfigTemplateService,
  private val sourceHandler: SourceHandler,
  @Named("jsonSecretsProcessorWithCopy") val secretsProcessor: JsonSecretsProcessor,
  private val connectionTemplateRepository: ConnectionTemplateRepository,
  private val workspaceHelper: WorkspaceHelper,
  private val workspaceRepository: WorkspaceRepository,
  private val connectionsHandler: ConnectionsHandler,
  private val destinationHandler: DestinationHandler,
  private val sourceService: SourceService,
  private val jobService: JobService,
  private val actorDefinitionService: ActorDefinitionService,
) {
  private val jsonMergingHelper = JsonMergingHelper()
  private val logger = KotlinLogging.logger {}

  /**
   * Creates a partial user config and its associated source.
   *
   * @param partialUserConfigCreate The updated partial user config
   * @return The created partial user config with actor details
   */
  fun createSourceFromPartialConfig(
    partialUserConfigCreate: PartialUserConfig,
    connectionConfiguration: JsonNode,
  ): SourceRead {
    // Get the config template and actor definition
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigCreate.configTemplateId)

    val combinedConfigs = combineDefaultAndUserConfig(configTemplate.configTemplate, connectionConfiguration)

    val sourceCreate =
      createSourceCreateFromPartialUserConfig(
        configTemplate.configTemplate,
        partialUserConfigCreate,
        combinedConfigs,
        configTemplate.actorName,
      )
    val sourceRead = sourceHandler.createSource(sourceCreate)

    val partialUserConfigToPersist =
      PartialUserConfig(
        id = sourceRead.sourceId,
        workspaceId = partialUserConfigCreate.workspaceId,
        configTemplateId = configTemplate.configTemplate.id,
        actorId = sourceRead.sourceId,
      )

    partialUserConfigService.createPartialUserConfig(partialUserConfigToPersist)

    // Create a configured schema and set the sync mode to incremental_append
    val schemaResponse: SourceDiscoverSchemaRead = sourceService.getSourceSchema(sourceRead.sourceId, false)
    val configuredSchema = schemaResponse.catalog

    for (stream in configuredSchema.streams) {
      stream.config.destinationSyncMode = DestinationSyncMode.APPEND
      if (stream.stream.supportedSyncModes.contains(SyncMode.INCREMENTAL) and stream.stream.sourceDefinedCursor) {
        // For the typical AI use case, INCREMENTAL_APPEND is the right mode as it'll allow Operators to process new data
        // We can make this configurable in the future as needed
        stream.config.syncMode = SyncMode.INCREMENTAL
        stream.config.cursorField = stream.stream.defaultCursorField
      }
    }

    // Create connection
    val destinations = destinationHandler.listDestinationsForWorkspace(WorkspaceIdRequestBody().workspaceId(partialUserConfigCreate.workspaceId))
    val organizationId = workspaceHelper.getOrganizationForWorkspace(partialUserConfigCreate.workspaceId)
    for (connectionTemplateEntity in connectionTemplateRepository.findByOrganizationIdAndTombstoneFalse(organizationId)) {
      val connectionTemplate = connectionTemplateEntity.toConfigModel()

      val matchingDestinations = destinations.destinations.filter { it.name == connectionTemplate.destinationName }
      if (matchingDestinations.size > 1) {
        throw IllegalStateException("Found multiple destinations with the same name: ${connectionTemplate.destinationName}. This is unexpected!")
      }
      val destination =
        matchingDestinations.firstOrNull()

      if (destination != null) {
        val namespaceFormat =
          if (connectionTemplate.namespaceDefinitionType == JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT &&
            connectionTemplate.namespaceFormat == null
          ) {
            workspaceRepository.findById(partialUserConfigToPersist.workspaceId).get().name
          } else {
            connectionTemplate.namespaceFormat
          }

        val defaultPrefix = "${sourceRead.sourceId}_"

        val createConnection =
          ConnectionCreate()
            .sourceId(sourceRead.sourceId)
            .destinationId(destination.destinationId)
            .namespaceDefinition(NamespaceDefinitionType.fromValue(connectionTemplate.namespaceDefinitionType.value()))
            .name("${sourceRead.name} -> ${destination.name}")
            .namespaceFormat(namespaceFormat)
            .status(ConnectionStatus.ACTIVE)
            .nonBreakingChangesPreference(NonBreakingChangesPreference.fromValue(connectionTemplate.nonBreakingChangesPreference.value()))
            .prefix(connectionTemplate.prefix ?: defaultPrefix)
            .syncCatalog(schemaResponse.catalog)
            .scheduleType(
              when (connectionTemplate.scheduleType) {
                StandardSync.ScheduleType.MANUAL -> ConnectionScheduleType.MANUAL
                StandardSync.ScheduleType.BASIC_SCHEDULE -> ConnectionScheduleType.BASIC
                StandardSync.ScheduleType.CRON -> ConnectionScheduleType.CRON
              },
            ).scheduleData(convertScheduleData(connectionTemplate.scheduleData))
        // FIXME tags are missing https://github.com/airbytehq/airbyte-internal-issues/issues/12810
        if (connectionTemplate.resourceRequirements != null) {
          createConnection.resourceRequirements(
            io.airbyte.api.model.generated
              .ResourceRequirements()
              .cpuLimit(connectionTemplate.resourceRequirements!!.cpuLimit)
              .cpuRequest(connectionTemplate.resourceRequirements!!.cpuRequest)
              .memoryLimit(connectionTemplate.resourceRequirements!!.memoryLimit)
              .memoryRequest(connectionTemplate.resourceRequirements!!.memoryRequest)
              .ephemeralStorageLimit(connectionTemplate.resourceRequirements!!.ephemeralStorageLimit)
              .ephemeralStorageRequest(connectionTemplate.resourceRequirements!!.ephemeralStorageRequest),
          )
        }
        val connection =
          connectionsHandler.createConnection(
            createConnection,
          )

        if (connectionTemplate.syncOnCreate) {
          val syncId = jobService.sync(connection.connectionId)
          logger.info(
            "Created connection: ${connection.connectionId} and started sync with id $syncId",
          )
        } else {
          logger.info("Created connection: ${connection.connectionId} but did not start sync")
        }
      } else {
        logger.info(
          "Did not create a connection for source ${sourceRead.sourceId} because no matching destination was found for ${connectionTemplate.destinationName}",
        )
      }
    }

    return sourceRead
  }

  private fun convertScheduleData(scheduleData: ScheduleData?): ConnectionScheduleData {
    val connectionScheduleData = ConnectionScheduleData()
    if (scheduleData == null) {
      return connectionScheduleData
    }
    if (scheduleData.basicSchedule != null) {
      connectionScheduleData.basicSchedule =
        ConnectionScheduleDataBasicSchedule()
          .timeUnit(
            ConnectionScheduleDataBasicSchedule.TimeUnitEnum.fromValue(scheduleData.basicSchedule.timeUnit.value()),
          ).units(scheduleData.basicSchedule.units)
    }
    if (scheduleData.cron != null) {
      connectionScheduleData.cron =
        ConnectionScheduleDataCron().cronExpression(scheduleData.cron.cronExpression).cronTimeZone(scheduleData.cron.cronTimeZone)
    }
    return connectionScheduleData
  }

  fun combineDefaultAndUserConfig(
    configTemplate: ConfigTemplate,
    userConfig: JsonNode,
  ): JsonNode {
    val maskedConfigTemplate =
      secretsProcessor.prepareSecretsForOutput(
        configTemplate.partialDefaultConfig,
        configTemplate.userConfigSpec.connectionSpecification,
      )

    return jsonMergingHelper.combineProperties(
      maskedConfigTemplate,
      userConfig,
    )
  }

  /**
   * Gets a partial config from a source
   *
   * @param partialUserConfigId The id of the partial user config
   * @return The fetched partial user config with its template
   */
  fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithFullDetails {
    val partialUserConfigStored = partialUserConfigService.getPartialUserConfig(partialUserConfigId)

    val sourceRead = sourceHandler.getSource(SourceIdRequestBody().apply { this.sourceId = partialUserConfigStored.partialUserConfig.actorId })
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfigStored.partialUserConfig.configTemplateId)

    val filteredConnectionConfiguration = filterConnectionConfigurationBySpec(sourceRead, configTemplate)

    val sanitizedConnectionConfiguration =
      secretsProcessor.prepareSecretsForOutput(
        filteredConnectionConfiguration,
        configTemplate.configTemplate.userConfigSpec.connectionSpecification,
      )

    val spec = getConnectorSpecification(ActorDefinitionId(configTemplate.configTemplate.actorDefinitionId))

    val combinedConfigsObject = sanitizedConnectionConfiguration as ObjectNode
    combinedConfigsObject.set<JsonNode>("advancedAuth", objectMapper.valueToTree(spec.advancedAuth))

    return PartialUserConfigWithFullDetails(
      partialUserConfig =
        PartialUserConfig(
          id = partialUserConfigStored.partialUserConfig.id,
          workspaceId = partialUserConfigStored.partialUserConfig.workspaceId,
          configTemplateId = configTemplate.configTemplate.id,
          actorId = sourceRead.sourceId,
        ),
      connectionConfiguration = sanitizedConnectionConfiguration,
      configTemplate = configTemplate.configTemplate,
      actorName = configTemplate.actorName,
      actorIcon = configTemplate.actorIcon,
    )
  }

  /**
   * Updates a source based on a partial user config.
   *
   * @param partialUserConfig The updated partial user config
   * @return The updated partial user config with actor details
   */
  fun updateSourceFromPartialConfig(
    partialUserConfig: PartialUserConfig,
    connectionConfiguration: JsonNode,
  ): SourceRead {
    val storedPartialUserConfig = partialUserConfigService.getPartialUserConfig(partialUserConfig.id)
    // Get the config template to use for merging properties
    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfig.configTemplateId)

    // Combine the template's default config and user config
    val combinedConfigs =
      jsonMergingHelper.combineProperties(
        ObjectMapper().valueToTree(configTemplate.configTemplate.partialDefaultConfig),
        ObjectMapper().valueToTree(connectionConfiguration),
      )

    val spec = getConnectorSpecification(ActorDefinitionId(configTemplate.configTemplate.actorDefinitionId))

    val combinedConfigsObject = combinedConfigs as ObjectNode
    combinedConfigsObject.set<JsonNode>("advancedAuth", objectMapper.valueToTree(spec.advancedAuth))

    // Update the source using the combined config
    // "Partial update" allows us to update the configuration without changing the name or other properties on the source

    val sourceRead =
      sourceHandler.partialUpdateSource(
        PartialSourceUpdate()
          .sourceId(storedPartialUserConfig.partialUserConfig.actorId)
          .connectionConfiguration(combinedConfigsObject),
      )

    return sourceRead
  }

  fun deletePartialUserConfig(partialUserConfigId: UUID) {
    val partialUserConfig = partialUserConfigService.getPartialUserConfig(partialUserConfigId)

    partialUserConfig.partialUserConfig.actorId?.let { sourceHandler.deleteSource(SourceIdRequestBody().apply { this.sourceId = it }) }
  }

  private fun createSourceCreateFromPartialUserConfig(
    configTemplate: ConfigTemplate,
    partialUserConfig: PartialUserConfig,
    combinedConfigs: JsonNode,
    actorName: String,
  ): SourceCreate =
    SourceCreate().apply {
      name = "$actorName ${partialUserConfig.workspaceId}"
      sourceDefinitionId = configTemplate.actorDefinitionId
      workspaceId = partialUserConfig.workspaceId
      connectionConfiguration = combinedConfigs
    }

  /**
   * Filters a source's connection configuration to only include
   * properties that are defined in the config template spec.
   *
   * @param sourceRead The SourceRead object containing the connection configuration
   * @param configTemplateRead The ConfigTemplateRead containing the user config specification
   * @return A JsonNode with only the properties specified in the template spec
   */
  @VisibleForTesting
  internal fun filterConnectionConfigurationBySpec(
    sourceRead: SourceRead,
    configTemplateRead: ConfigTemplateWithActorDetails,
  ): JsonNode {
    val connectionConfig = sourceRead.connectionConfiguration

    val userConfigSpec = configTemplateRead.configTemplate.userConfigSpec.connectionSpecification

    // Convert ConnectorSpecification to JsonNode
    val specAsJson = objectMapper.valueToTree<JsonNode>(userConfigSpec)

    // Filter the connection configuration based on the schema
    return filterJsonNodeBySchema(connectionConfig, specAsJson)
  }

  /**
   * Recursively filters a JsonNode according to a JSON schema
   */
  @VisibleForTesting
  internal fun filterJsonNodeBySchema(
    node: JsonNode,
    schema: JsonNode,
  ): JsonNode {
    // If schema doesn't exist or is not an object, return an empty object
    if (!schema.isObject) {
      return objectMapper.createObjectNode()
    }

    // Check schema type
    val schemaType = schema.get("type")?.asText()

    // Handle different schema types
    when (schemaType) {
      "object" -> {
        // Handle object schema
        val filteredNode = objectMapper.createObjectNode()

        // Process schema properties if they exist
        if (schema.has("properties") && node.isObject) {
          val propertiesSchema = schema.get("properties")

          for (fieldName in node.fieldNames().asSequence()) {
            // Check if this field is defined in the schema
            if (propertiesSchema.has(fieldName)) {
              val fieldValue = node.get(fieldName)
              val fieldSchema = propertiesSchema.get(fieldName)

              // Recursively filter the field
              filteredNode.set<JsonNode>(fieldName, filterJsonNodeBySchema(fieldValue, fieldSchema))
            }
          }
        }

        if (schema.has("oneOf")) {
          val oneOfArray = schema.get("oneOf")
          if (oneOfArray.isArray && node.isObject) {
            // Find which schema best matches our input
            var bestMatchSchema: JsonNode? = null
            var maxMatchCount = -1

            for (subSchema in oneOfArray) {
              if (!subSchema.has("properties")) continue

              val schemaProps = subSchema.get("properties")
              val nodeFields = node.fieldNames().asSequence().toSet()
              val schemaFields = schemaProps.fieldNames().asSequence().toSet()

              val commonFields = nodeFields.intersect(schemaFields)
              val matchScore = commonFields.size

              if (commonFields == nodeFields) {
                // Perfect match - all fields are covered
                bestMatchSchema = subSchema
                break
              } else if (matchScore > maxMatchCount) {
                bestMatchSchema = subSchema
                maxMatchCount = matchScore
              }
            }

            // If we found a matching schema, ONLY include properties from that schema
            if (bestMatchSchema != null && bestMatchSchema.has("properties")) {
              val resultNode = objectMapper.createObjectNode()
              val schemaProps = bestMatchSchema.get("properties")
              val schemaFieldNames = schemaProps.fieldNames()

              // Only include properties explicitly defined in the schema
              while (schemaFieldNames.hasNext()) {
                val propName = schemaFieldNames.next()
                if (node.has(propName)) {
                  val propSchema = schemaProps.get(propName)
                  resultNode.set<JsonNode>(propName, filterJsonNodeBySchema(node.get(propName), propSchema))
                }
              }

              return resultNode // Return only the properties defined in the matching schema
            }
          }
        }
        return filteredNode
      }
      "array" -> {
        // For arrays, if node is an array, process each element with the items schema
        if (node.isArray && schema.has("items")) {
          val itemsSchema = schema.get("items")
          val arrayNode = objectMapper.createArrayNode()

          node.forEach { element ->
            arrayNode.add(filterJsonNodeBySchema(element, itemsSchema))
          }

          return arrayNode
        }

        // If the node is not an array but schema expects array, return empty array
        return objectMapper.createArrayNode()
      }
      else -> {
        // For primitive types (string, number, boolean, null) or when type is not specified,
        // simply return the node
        return node
      }
    }
  }

  private fun getConnectorSpecification(actorDefinitionId: ActorDefinitionId): ConnectorSpecification {
    val actorDefinition =
      actorDefinitionService
        .getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId.value)
        .orElseThrow { throw RuntimeException("ActorDefinition not found") }
    val actorDefinitionSpec = actorDefinition.spec
    return actorDefinitionSpec
  }
}
