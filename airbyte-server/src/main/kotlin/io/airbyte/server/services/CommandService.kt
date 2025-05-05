/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.config.ConnectorJobOutput
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.NotFoundException
import io.airbyte.workload.services.WorkloadService
import jakarta.inject.Singleton

enum class CommandStatus {
  PENDING,
  RUNNING,
  COMPLETED,
  CANCELLED,
}

@Singleton
class CommandService(
  private val commandsRepository: CommandsRepository,
  private val workloadService: WorkloadService,
  private val workloadOutputReader: WorkloadOutputDocStoreReader,
) {
  fun cancel(commandId: String) {
    commandsRepository.findById(commandId).ifPresent { command ->
      workloadService.cancelWorkload(command.workloadId, "api", "cancelled from the api")
    }
  }

  fun getStatus(commandId: String): CommandStatus? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        try {
          when (workloadService.getWorkload(command.workloadId).status) {
            WorkloadStatus.PENDING -> CommandStatus.PENDING
            WorkloadStatus.CLAIMED -> CommandStatus.PENDING
            WorkloadStatus.LAUNCHED -> CommandStatus.PENDING
            WorkloadStatus.RUNNING -> CommandStatus.RUNNING
            WorkloadStatus.SUCCESS -> CommandStatus.COMPLETED
            WorkloadStatus.FAILURE -> CommandStatus.COMPLETED
            WorkloadStatus.CANCELLED -> CommandStatus.CANCELLED
          }
        } catch (e: NotFoundException) {
          null
        }
      }.orElse(null)

  fun getConnectorJobOutput(commandId: String): ConnectorJobOutput? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        workloadOutputReader.readConnectorOutput(command.workloadId)
      }.orElse(null)
}
