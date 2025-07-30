/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.collect.Lists
import io.airbyte.api.model.generated.CheckOperationRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.OperationCreate
import io.airbyte.api.model.generated.OperationIdRequestBody
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperationReadList
import io.airbyte.api.model.generated.OperationUpdate
import io.airbyte.api.model.generated.OperatorConfiguration
import io.airbyte.api.model.generated.OperatorType
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.converters.OperationsConverter.operationReadFromPersistedOperation
import io.airbyte.commons.server.converters.OperationsConverter.populateOperatorConfigFromApi
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.function.Supplier

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class OperationsHandler
  @VisibleForTesting
  internal constructor(
    private val workspaceService: WorkspaceService,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    private val connectionService: ConnectionService,
    private val operationService: OperationService,
  ) {
    @Inject
    constructor(
      workspaceService: WorkspaceService,
      operationService: OperationService,
      connectionService: ConnectionService,
    ) : this(workspaceService, Supplier<UUID> { UUID.randomUUID() }, connectionService, operationService)

    fun checkOperation(operationCheck: OperatorConfiguration): CheckOperationRead {
      try {
        validateOperation(operationCheck)
      } catch (e: IllegalArgumentException) {
        return CheckOperationRead()
          .status(CheckOperationRead.StatusEnum.FAILED)
          .message(e.message)
      }
      return CheckOperationRead().status(CheckOperationRead.StatusEnum.SUCCEEDED)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun createOperation(operationCreate: OperationCreate): OperationRead {
      val operationId = uuidGenerator.get()
      val workspace: StandardWorkspace
      try {
        workspace = workspaceService.getWorkspaceWithSecrets(operationCreate.workspaceId, false)
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException("WORKSPACE", operationCreate.workspaceId.toString())
      }
      val standardSyncOperation =
        toStandardSyncOperation(operationCreate, workspace)
          .withOperationId(operationId)
      return persistOperation(standardSyncOperation)
    }

    private fun toStandardSyncOperation(
      operationCreate: OperationCreate,
      workspace: StandardWorkspace,
    ): StandardSyncOperation {
      val standardSyncOperation =
        StandardSyncOperation()
          .withWorkspaceId(operationCreate.workspaceId)
          .withName(operationCreate.name)
          .withOperatorType(operationCreate.operatorConfiguration.operatorType?.convertTo<StandardSyncOperation.OperatorType>())
          .withTombstone(false)
      populateOperatorConfigFromApi(operationCreate.operatorConfiguration, standardSyncOperation, workspace)
      return standardSyncOperation
    }

    private fun validateOperation(operatorConfiguration: OperatorConfiguration) {
      if (OperatorType.WEBHOOK == operatorConfiguration.operatorType) {
        Preconditions.checkArgument(operatorConfiguration.webhook != null)
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun updateOperation(operationUpdate: OperationUpdate): OperationRead {
      val standardSyncOperation = operationService.getStandardSyncOperation(operationUpdate.operationId)
      val workspace: StandardWorkspace
      try {
        workspace = workspaceService.getWorkspaceWithSecrets(standardSyncOperation.workspaceId, false)
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException("WORKSPACE", standardSyncOperation.workspaceId.toString())
      }
      return persistOperation(updateOperation(operationUpdate, standardSyncOperation, workspace))
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun persistOperation(standardSyncOperation: StandardSyncOperation): OperationRead {
      operationService.writeStandardSyncOperation(standardSyncOperation)
      return buildOperationRead(standardSyncOperation.operationId)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun listOperationsForConnection(connectionIdRequestBody: ConnectionIdRequestBody): OperationReadList {
      val operationReads: MutableList<OperationRead> = Lists.newArrayList()
      val standardSync = connectionService.getStandardSync(connectionIdRequestBody.connectionId)
      for (operationId in standardSync.operationIds) {
        val standardSyncOperation = operationService.getStandardSyncOperation(operationId)
        if (standardSyncOperation.tombstone != null && standardSyncOperation.tombstone) {
          continue
        }
        operationReads.add(buildOperationRead(standardSyncOperation))
      }
      return OperationReadList().operations(operationReads)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getOperation(operationIdRequestBody: OperationIdRequestBody): OperationRead = buildOperationRead(operationIdRequestBody.operationId)

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun deleteOperationsForConnection(connectionIdRequestBody: ConnectionIdRequestBody) {
      val standardSync = connectionService.getStandardSync(connectionIdRequestBody.connectionId)
      deleteOperationsForConnection(standardSync, standardSync.operationIds)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun deleteOperationsForConnection(
      connectionId: UUID,
      deleteOperationIds: List<UUID>,
    ) {
      val standardSync = connectionService.getStandardSync(connectionId)
      deleteOperationsForConnection(standardSync, deleteOperationIds)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun deleteOperationsForConnection(
      standardSync: StandardSync,
      deleteOperationIds: List<UUID>,
    ) {
      val operationIds: MutableList<UUID> = ArrayList(standardSync.operationIds)
      for (operationId in deleteOperationIds) {
        operationIds.remove(operationId)
        var sharedOperation = false
        for (sync in connectionService.listStandardSyncsUsingOperation(operationId)) {
          // Check if other connections are using the same operation
          if (sync.connectionId != standardSync.connectionId) {
            sharedOperation = true
            break
          }
        }
        if (!sharedOperation) {
          removeOperation(operationId)
        }
      }

      operationService.updateConnectionOperationIds(standardSync.connectionId, HashSet(operationIds))
    }

    @Throws(IOException::class)
    fun deleteOperation(operationIdRequestBody: OperationIdRequestBody) {
      val operationId = operationIdRequestBody.operationId
      operationService.deleteStandardSyncOperation(operationId)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    private fun removeOperation(operationId: UUID) {
      val standardSyncOperation = operationService.getStandardSyncOperation(operationId)
      if (standardSyncOperation != null) {
        standardSyncOperation.withTombstone(true)
        persistOperation(standardSyncOperation)
      } else {
        throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC_OPERATION, operationId.toString())
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildOperationRead(operationId: UUID): OperationRead {
      val standardSyncOperation = operationService.getStandardSyncOperation(operationId)
      if (standardSyncOperation != null) {
        return buildOperationRead(standardSyncOperation)
      } else {
        throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC_OPERATION, operationId.toString())
      }
    }

    companion object {
      fun updateOperation(
        operationUpdate: OperationUpdate,
        standardSyncOperation: StandardSyncOperation,
        workspace: StandardWorkspace,
      ): StandardSyncOperation {
        standardSyncOperation
          .withName(operationUpdate.name)
        populateOperatorConfigFromApi(operationUpdate.operatorConfiguration, standardSyncOperation, workspace)
        return standardSyncOperation
      }

      private fun buildOperationRead(standardSyncOperation: StandardSyncOperation): OperationRead =
        operationReadFromPersistedOperation(standardSyncOperation)
    }
  }
