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
  fun getStandardSyncOperation(operationId: UUID): StandardSyncOperation

  fun writeStandardSyncOperation(standardSyncOperation: StandardSyncOperation)

  fun listStandardSyncOperations(): List<StandardSyncOperation>

  fun updateConnectionOperationIds(
    connectionId: UUID,
    newOperationIds: Set<UUID>,
  )

  fun deleteStandardSyncOperation(standardSyncOperationId: UUID)
}
