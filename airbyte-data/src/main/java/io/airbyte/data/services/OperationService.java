/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.StandardSyncOperation;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Operation Service.
 */
public interface OperationService {

  StandardSyncOperation getStandardSyncOperation(UUID operationId) throws JsonValidationException, IOException, ConfigNotFoundException;

  void writeStandardSyncOperation(StandardSyncOperation standardSyncOperation) throws IOException;

  List<StandardSyncOperation> listStandardSyncOperations() throws IOException;

  void updateConnectionOperationIds(UUID connectionId, Set<UUID> newOperationIds) throws IOException;

  void deleteStandardSyncOperation(UUID standardSyncOperationId) throws IOException;

}
