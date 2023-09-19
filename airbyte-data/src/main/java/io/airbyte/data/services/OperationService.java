/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Operation Service.
 */
public interface OperationService {

  Stream<StandardSyncOperation> listStandardSyncOperationQuery(Optional<UUID> configId) throws IOException;

  StandardSyncOperation getStandardSyncOperation(UUID operationId) throws JsonValidationException, IOException, ConfigNotFoundException;

  void writeStandardSyncOperation(StandardSyncOperation standardSyncOperation) throws IOException;

  void writeStandardSyncOperation(List<StandardSyncOperation> configs);

  List<StandardSyncOperation> listStandardSyncOperations() throws IOException;

  void updateConnectionOperationIds(UUID connectionId, Set<UUID> newOperationIds) throws IOException;

  void deleteStandardSyncOperation(UUID standardSyncOperationId) throws IOException;

}
