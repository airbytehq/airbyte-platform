/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.public_api.model.generated.ConnectionResponse
import io.airbyte.public_api.model.generated.ConnectionScheduleResponse
import io.airbyte.public_api.model.generated.ConnectionStatusEnum
import io.airbyte.public_api.model.generated.ConnectionSyncModeEnum
import io.airbyte.public_api.model.generated.GeographyEnum
import io.airbyte.public_api.model.generated.NamespaceDefinitionEnum
import io.airbyte.public_api.model.generated.NonBreakingSchemaUpdatesBehaviorEnum
import io.airbyte.public_api.model.generated.ScheduleTypeWithBasicEnum
import io.airbyte.public_api.model.generated.StreamConfiguration
import io.airbyte.public_api.model.generated.StreamConfigurations
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
    val connectionResponse = ConnectionResponse()
    connectionResponse.setConnectionId(connectionRead.connectionId)
    connectionResponse.setName(connectionRead.name)
    connectionResponse.setSourceId(connectionRead.sourceId)
    connectionResponse.setDestinationId(connectionRead.destinationId)
    connectionResponse.setWorkspaceId(workspaceId)
    connectionResponse.setStatus(ConnectionStatusEnum.fromValue(connectionRead.status.name))
    if (connectionRead.geography != null) {
      connectionResponse.setDataResidency(GeographyEnum.fromValue(connectionRead.geography.name))
    }
    val connectionScheduleResponse = ConnectionScheduleResponse()
    if (connectionRead.namespaceDefinition != null) {
      connectionResponse.setNamespaceDefinition(convertNamespaceDefinitionType(connectionRead.namespaceDefinition))
    }
    if (connectionRead.namespaceFormat != null) {
      connectionResponse.setNamespaceFormat(connectionRead.namespaceFormat)
    }
    if (connectionRead.prefix != null) {
      connectionResponse.setPrefix(connectionRead.prefix)
    }
    if (connectionRead.nonBreakingChangesPreference != null) {
      connectionResponse.setNonBreakingSchemaUpdatesBehavior(convertNonBreakingChangesPreference(connectionRead.nonBreakingChangesPreference))
    }

    // connectionRead.getSchedule() is soon to be deprecated, but has not been cleaned up in the
    // database yet
    if (connectionRead.schedule != null) {
      connectionScheduleResponse.setScheduleType(ScheduleTypeWithBasicEnum.BASIC)
      // should this string just be a json object?
      val basicTimingString = "Every " + connectionRead.schedule!!.units + " " + connectionRead.schedule!!.timeUnit.name
      connectionScheduleResponse.setBasicTiming(basicTimingString)
    } else if (connectionRead.schedule != null && connectionRead.scheduleType == null) {
      connectionScheduleResponse.setScheduleType(ScheduleTypeWithBasicEnum.MANUAL)
    }
    if (connectionRead.scheduleType == ConnectionScheduleType.MANUAL) {
      connectionScheduleResponse.setScheduleType(ScheduleTypeWithBasicEnum.MANUAL)
    } else if (connectionRead.scheduleType == ConnectionScheduleType.CRON) {
      connectionScheduleResponse.setScheduleType(ScheduleTypeWithBasicEnum.CRON)
      if (connectionRead.scheduleData != null && connectionRead.scheduleData!!.cron != null) {
        // should this string just be a json object?
        val cronExpressionWithTimezone =
          connectionRead.scheduleData!!.cron!!
            .cronExpression + " " + connectionRead.scheduleData!!.cron!!.cronTimeZone
        connectionScheduleResponse.setCronExpression(cronExpressionWithTimezone)
      } else {
//        ConnectionReadMapper.log.error("CronExpression not found in ScheduleData for connection: {}", connectionRead.connectionId)
      }
    } else if (connectionRead.scheduleType == ConnectionScheduleType.BASIC) {
      connectionScheduleResponse.setScheduleType(ScheduleTypeWithBasicEnum.BASIC)
      if (connectionRead.scheduleData != null && connectionRead.scheduleData!!.basicSchedule != null) {
        val schedule = connectionRead.scheduleData!!.basicSchedule
        val basicTimingString = "Every " + schedule!!.units + " " + schedule.timeUnit.name
        connectionScheduleResponse.setBasicTiming(basicTimingString)
      } else {
//        ConnectionReadMapper.log.error("BasicSchedule not found in ScheduleData for connection: {}", connectionRead.connectionId)
      }
    }
    if (connectionRead.syncCatalog != null) {
      val streamConfigurations = StreamConfigurations()
      for (streamAndConfiguration in connectionRead.syncCatalog.streams) {
        assert(streamAndConfiguration.config != null)
        val connectionSyncMode: ConnectionSyncModeEnum? =
          syncModesToConnectionSyncModeEnum(
            streamAndConfiguration.config!!.syncMode,
            streamAndConfiguration.config!!.destinationSyncMode,
          )
        streamConfigurations.addStreamsItem(
          StreamConfiguration()
            .name(streamAndConfiguration.stream!!.name)
            .primaryKey(streamAndConfiguration.config!!.primaryKey)
            .cursorField(streamAndConfiguration.config!!.cursorField)
            .syncMode(connectionSyncMode),
        )
      }
      connectionResponse.setConfigurations(streamConfigurations)
    }
    connectionResponse.setSchedule(connectionScheduleResponse)
    return connectionResponse
  }

  private fun convertNamespaceDefinitionType(namespaceDefinitionType: NamespaceDefinitionType?): NamespaceDefinitionEnum {
    return if (namespaceDefinitionType == NamespaceDefinitionType.CUSTOMFORMAT) {
      NamespaceDefinitionEnum.CUSTOM_FORMAT
    } else {
      NamespaceDefinitionEnum.fromValue(namespaceDefinitionType.toString())
    }
  }

  private fun convertNonBreakingChangesPreference(nonBreakingChangesPreference: NonBreakingChangesPreference?): NonBreakingSchemaUpdatesBehaviorEnum {
    return if (nonBreakingChangesPreference == NonBreakingChangesPreference.DISABLE) {
      NonBreakingSchemaUpdatesBehaviorEnum.DISABLE_CONNECTION
    } else {
      NonBreakingSchemaUpdatesBehaviorEnum.fromValue(nonBreakingChangesPreference.toString())
    }
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
    val mapper: MutableMap<SyncMode, Map<DestinationSyncMode, ConnectionSyncModeEnum>>? =
      java.util.Map.of(
        SyncMode.FULL_REFRESH,
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
        SyncMode.INCREMENTAL,
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
    return if (sourceSyncMode == null || destinationSyncMode == null || mapper!![sourceSyncMode] == null) {
      ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE
    } else {
      mapper[sourceSyncMode]!![destinationSyncMode]
    }
  }
}
