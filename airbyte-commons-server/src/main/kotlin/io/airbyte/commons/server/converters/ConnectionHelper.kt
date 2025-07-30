/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.google.common.base.Preconditions
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.BasicSchedule
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.validation.json.JsonValidationException
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

// todo (cgardens) - we are not getting any value out of instantiating this class. we should just
// use it as statics. not doing it now, because already in the middle of another refactor.

/**
 * Connection Helpers.
 */
@Singleton
class ConnectionHelper(
  private val connectionService: ConnectionService,
  private val workspaceHelper: WorkspaceHelper,
) {
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun deleteConnection(connectionId: UUID) {
    val update = Jsons.clone(connectionService.getStandardSync(connectionId).withStatus(StandardSync.Status.DEPRECATED))
    updateConnection(update)
  }

  /**
   * Given a connection update, fetches an existing connection, applies the update, and then persists
   * the update.
   *
   * @param update - updated sync info to be merged with original sync.
   * @return new sync object
   * @throws JsonValidationException - if provided object is invalid
   * @throws ConfigNotFoundException - if there is no sync already persisted
   * @throws IOException - you never know when you io
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun updateConnection(update: StandardSync): StandardSync {
    val original = connectionService.getStandardSync(update.connectionId)
    val newConnection = updateConnectionObject(workspaceHelper, original, update)
    connectionService.writeStandardSync(newConnection)
    return newConnection
  }

  @JvmRecord
  data class StreamName(
    val namespace: String?,
    val name: String,
  )

  companion object {
    /**
     * Core logic for merging an existing connection configuration with an update.
     *
     * @param workspaceHelper - helper class
     * @param original - already persisted sync
     * @param update - updated sync info to be merged with original sync.
     * @return new sync object
     */
    fun updateConnectionObject(
      workspaceHelper: WorkspaceHelper,
      original: StandardSync,
      update: StandardSync,
    ): StandardSync {
      validateWorkspace(workspaceHelper, original.sourceId, original.destinationId, update.operationIds)

      val newConnection =
        Jsons
          .clone(original)
          .withNamespaceDefinition(
            update.namespaceDefinition?.convertTo<JobSyncConfig.NamespaceDefinitionType>(),
          ).withNamespaceFormat(update.namespaceFormat)
          .withPrefix(update.prefix)
          .withOperationIds(update.operationIds)
          .withCatalog(update.catalog)
          .withStatus(update.status)
          .withSourceCatalogId(update.sourceCatalogId)

      // update name
      if (update.name != null) {
        newConnection.withName(update.name)
      }

      // update Resource Requirements
      if (update.resourceRequirements != null) {
        newConnection.withResourceRequirements(Jsons.clone(update.resourceRequirements))
      } else {
        newConnection.withResourceRequirements(original.resourceRequirements)
      }

      if (update.scheduleType != null) {
        newConnection.withScheduleType(update.scheduleType)
        newConnection.withManual(update.manual)
        if (update.scheduleData != null) {
          newConnection.withScheduleData(Jsons.clone(update.scheduleData))
        }
      } else if (update.schedule != null) {
        val newSchedule =
          Schedule()
            .withTimeUnit(update.schedule.timeUnit)
            .withUnits(update.schedule.units)
        newConnection.withManual(false).withSchedule(newSchedule)
        // Also write into the new field. This one will be consumed if populated.
        newConnection
          .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
        newConnection.withScheduleData(
          ScheduleData().withBasicSchedule(
            BasicSchedule()
              .withTimeUnit(convertTimeUnitSchema(update.schedule.timeUnit))
              .withUnits(update.schedule.units),
          ),
        )
      } else {
        newConnection.withManual(true).withSchedule(null)
        newConnection.withScheduleType(StandardSync.ScheduleType.MANUAL).withScheduleData(null)
      }

      return newConnection
    }

    /**
     * Validate all resources are from the same workspace.
     *
     * @param workspaceHelper workspace helper
     * @param sourceId source id
     * @param destinationId destination id
     * @param operationIds operation ids
     */
    @JvmStatic
    fun validateWorkspace(
      workspaceHelper: WorkspaceHelper,
      sourceId: UUID,
      destinationId: UUID,
      @Nullable operationIds: List<UUID>?,
    ) {
      val sourceWorkspace = workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(sourceId)
      val destinationWorkspace = workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)

      Preconditions.checkArgument(
        sourceWorkspace == destinationWorkspace,
        String.format(
          (
            "Source and destination do not belong to the same workspace. " +
              "Source id: %s, " +
              "Source workspace id: %s, " +
              "Destination id: %s, " +
              "Destination workspace id: %s"
          ),
          sourceId,
          sourceWorkspace,
          destinationId,
          destinationWorkspace,
        ),
      )

      if (operationIds != null) {
        for (operationId in operationIds) {
          val operationWorkspace = workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)
          Preconditions.checkArgument(
            sourceWorkspace == operationWorkspace,
            String.format(
              "Operation and connection do not belong to the same workspace. Workspace id: %s, Operation id: %s, Operation workspace id: %s",
              sourceWorkspace,
              operationId,
              operationWorkspace,
            ),
          )
        }
      }
    }

    // Helper method to convert between TimeUnit enums for old and new schedule schemas.
    private fun convertTimeUnitSchema(timeUnit: Schedule.TimeUnit): BasicSchedule.TimeUnit =
      when (timeUnit) {
        Schedule.TimeUnit.MINUTES -> BasicSchedule.TimeUnit.MINUTES
        Schedule.TimeUnit.HOURS -> BasicSchedule.TimeUnit.HOURS
        Schedule.TimeUnit.DAYS -> BasicSchedule.TimeUnit.DAYS
        Schedule.TimeUnit.WEEKS -> BasicSchedule.TimeUnit.WEEKS
        Schedule.TimeUnit.MONTHS -> BasicSchedule.TimeUnit.MONTHS
        else -> throw RuntimeException("Unhandled TimeUnitEnum: $timeUnit")
      }

    @JvmStatic
    fun validateCatalogDoesntContainDuplicateStreamNames(syncCatalog: AirbyteCatalog) {
      val streamNames: MutableSet<StreamName> = HashSet()
      for (s in syncCatalog.streams) {
        val stream = s.stream
        require(streamNames.add(StreamName(stream.namespace, stream.name))) {
          String.format(
            "Catalog contains duplicate stream names: %s.%s",
            stream.namespace,
            stream.name,
          )
        }
      }
    }
  }
}
