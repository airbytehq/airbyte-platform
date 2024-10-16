/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.publicApi.server.generated.models.ConnectionScheduleResponse
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.ConnectionSyncModeEnum
import io.airbyte.publicApi.server.generated.models.GeographyEnum
import io.airbyte.publicApi.server.generated.models.NamespaceDefinitionEnum
import io.airbyte.publicApi.server.generated.models.NonBreakingSchemaUpdatesBehaviorEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeWithBasicEnum
import io.airbyte.publicApi.server.generated.models.SelectedFieldInfo
import io.airbyte.publicApi.server.generated.models.StreamConfiguration
import io.airbyte.publicApi.server.generated.models.StreamConfigurations
import io.airbyte.publicApi.server.generated.models.StreamMapperType
import java.util.UUID

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object ConnectionReadMapper {
  /**
   * Converts a ConnectionRead object from the config api to a ConnectionResponse.
   *
   * @param connectionRead Output of a connection create/get from config api
   * @return ConnectionResponse Response object
   */
  fun from(
    connectionRead: ConnectionRead,
    workspaceId: UUID?,
  ): ConnectionResponse {
    val streamConfigurations =
      connectionRead.syncCatalog?.let { catalog ->
        StreamConfigurations(
          streams =
            catalog.streams.map { streamAndConfiguration ->
              assert(streamAndConfiguration.config != null)
              val connectionSyncMode: ConnectionSyncModeEnum? =
                syncModesToConnectionSyncModeEnum(
                  streamAndConfiguration.config!!.syncMode,
                  streamAndConfiguration.config!!.destinationSyncMode,
                )
              val selectedFields: List<SelectedFieldInfo>? =
                streamAndConfiguration.config!!.selectedFields?.map {
                  selectedFieldInfoConverter(it)
                }
              StreamConfiguration(
                name = streamAndConfiguration.stream.name,
                primaryKey = streamAndConfiguration.config.primaryKey,
                cursorField = streamAndConfiguration.config.cursorField,
                mappers = convertMappers(streamAndConfiguration.config.mappers),
                syncMode = connectionSyncMode,
                selectedFields = selectedFields,
              )
            }.toList(),
        )
      } ?: StreamConfigurations()

    val connectionScheduleResponse =
      ConnectionScheduleResponse(
        scheduleType = getScheduleType(connectionRead = connectionRead),
        cronExpression = connectionRead.scheduleData?.let { d -> d.cron?.let { c -> "${c.cronExpression} ${c.cronTimeZone}" } },
        basicTiming =
          connectionRead.scheduleType?.let { t ->
            if (t == ConnectionScheduleType.BASIC) {
              connectionRead.scheduleData?.let { s -> s.basicSchedule?.let { b -> "Every ${b.units} ${b.timeUnit.name}" } }
            } else {
              null
            }
          },
      )

    return ConnectionResponse(
      connectionId = connectionRead.connectionId.toString(),
      name = connectionRead.name,
      sourceId = connectionRead.sourceId.toString(),
      destinationId = connectionRead.destinationId.toString(),
      workspaceId = workspaceId.toString(),
      status = ConnectionStatusEnum.valueOf(connectionRead.status.toString().uppercase()),
      schedule = connectionScheduleResponse,
      dataResidency = connectionRead.geography?.let { g -> GeographyEnum.valueOf(g.toString().uppercase()) } ?: GeographyEnum.AUTO,
      configurations = streamConfigurations,
      nonBreakingSchemaUpdatesBehavior = connectionRead.nonBreakingChangesPreference?.let { n -> convertNonBreakingChangesPreference(n) },
      namespaceDefinition = connectionRead.namespaceDefinition?.let { n -> convertNamespaceDefinitionType(n) },
      namespaceFormat = connectionRead.namespaceFormat,
      prefix = connectionRead.prefix,
    )
  }

  private fun getScheduleType(connectionRead: ConnectionRead): ScheduleTypeWithBasicEnum {
    return if (connectionRead.schedule != null) {
      ScheduleTypeWithBasicEnum.BASIC
    } else if (connectionRead.schedule != null && connectionRead.scheduleType == null) {
      ScheduleTypeWithBasicEnum.MANUAL
    } else if (connectionRead.scheduleType == ConnectionScheduleType.MANUAL) {
      ScheduleTypeWithBasicEnum.MANUAL
    } else if (connectionRead.scheduleType == ConnectionScheduleType.CRON) {
      ScheduleTypeWithBasicEnum.CRON
    } else if (connectionRead.scheduleType == ConnectionScheduleType.BASIC) {
      ScheduleTypeWithBasicEnum.BASIC
    } else {
      ScheduleTypeWithBasicEnum.BASIC
    }
  }

  private fun convertNamespaceDefinitionType(namespaceDefinitionType: NamespaceDefinitionType): NamespaceDefinitionEnum {
    return if (namespaceDefinitionType == NamespaceDefinitionType.CUSTOMFORMAT) {
      NamespaceDefinitionEnum.CUSTOM_FORMAT
    } else {
      NamespaceDefinitionEnum.valueOf(namespaceDefinitionType.toString().uppercase())
    }
  }

  private fun convertNonBreakingChangesPreference(nonBreakingChangesPreference: NonBreakingChangesPreference?): NonBreakingSchemaUpdatesBehaviorEnum {
    return if (nonBreakingChangesPreference == NonBreakingChangesPreference.DISABLE) {
      NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION
    } else {
      NonBreakingSchemaUpdatesBehaviorEnum.valueOf(nonBreakingChangesPreference.toString().uppercase())
    }
  }

  private fun convertMappers(mappers: List<ConfiguredStreamMapper>?): List<io.airbyte.publicApi.server.generated.models.ConfiguredStreamMapper>? {
    return mappers?.map { mapper ->
      io.airbyte.publicApi.server.generated.models.ConfiguredStreamMapper(
        type = StreamMapperType.decode(mapper.type.toString()) ?: throw IllegalArgumentException("Invalid stream mapper type"),
        mapperConfiguration = mapper.mapperConfiguration,
      )
    }
  }

  /**
   * Convert selected fields from airbyte_api model to public_api model
   * */
  private fun selectedFieldInfoConverter(selectedField: io.airbyte.api.model.generated.SelectedFieldInfo): SelectedFieldInfo {
    return SelectedFieldInfo(
      fieldPath = selectedField.fieldPath,
    )
  }

  /**
   * Map sync modes to combined sync modes.
   *
   * @param sourceSyncMode - source sync mode
   * @param destinationSyncMode - destination sync mode
   * @return - ConnectionSyncModeEnum value that corresponds. Null if there isn't one.
   */
  fun syncModesToConnectionSyncModeEnum(
    sourceSyncMode: SyncMode?,
    destinationSyncMode: DestinationSyncMode?,
  ): ConnectionSyncModeEnum? {
    val mapper: MutableMap<SyncMode, Map<DestinationSyncMode, ConnectionSyncModeEnum>> =
      mutableMapOf(
        SyncMode.FULL_REFRESH to
          mapOf(
            Pair(
              DestinationSyncMode.OVERWRITE,
              ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE,
            ),
            Pair(
              DestinationSyncMode.APPEND,
              ConnectionSyncModeEnum.FULL_REFRESH_APPEND,
            ),
          ),
        SyncMode.INCREMENTAL to
          mapOf(
            Pair(
              DestinationSyncMode.APPEND,
              ConnectionSyncModeEnum.INCREMENTAL_APPEND,
            ),
            Pair(
              DestinationSyncMode.APPEND_DEDUP,
              ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY,
            ),
          ),
      )
    return if (sourceSyncMode == null || destinationSyncMode == null || mapper[sourceSyncMode] == null) {
      ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE
    } else {
      mapper[sourceSyncMode]!![destinationSyncMode]
    }
  }
}
