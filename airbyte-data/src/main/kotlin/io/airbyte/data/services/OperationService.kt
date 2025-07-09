/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.StandardSyncOperation
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.UUID

/**
 * Operation Service.
 */
interface OperationService {
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardSyncOperation(operationId: UUID): StandardSyncOperation

  @Throws(IOException::class)
  fun writeStandardSyncOperation(standardSyncOperation: StandardSyncOperation)

  @Throws(IOException::class)
  fun listStandardSyncOperations(): List<StandardSyncOperation>

  @Throws(IOException::class)
  fun updateConnectionOperationIds(
    connectionId: UUID,
    newOperationIds: Set<UUID>,
  )

  @Throws(IOException::class)
  fun deleteStandardSyncOperation(standardSyncOperationId: UUID)
}
