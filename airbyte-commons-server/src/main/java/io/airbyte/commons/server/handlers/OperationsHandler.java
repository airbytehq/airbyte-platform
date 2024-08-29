/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.CheckOperationRead;
import io.airbyte.api.model.generated.CheckOperationRead.StatusEnum;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.OperationCreate;
import io.airbyte.api.model.generated.OperationIdRequestBody;
import io.airbyte.api.model.generated.OperationRead;
import io.airbyte.api.model.generated.OperationReadList;
import io.airbyte.api.model.generated.OperationUpdate;
import io.airbyte.api.model.generated.OperatorConfiguration;
import io.airbyte.commons.converters.OperationsConverter;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class OperationsHandler {

  private final ConfigRepository configRepository;

  private final WorkspaceService workspaceService;
  private final Supplier<UUID> uuidGenerator;

  @Inject
  public OperationsHandler(final ConfigRepository configRepository, WorkspaceService workspaceService) {
    this(configRepository, workspaceService, UUID::randomUUID);
  }

  @VisibleForTesting
  OperationsHandler(final ConfigRepository configRepository,
                    final WorkspaceService workspaceService,
                    @Named("uuidGenerator") final Supplier<UUID> uuidGenerator) {
    this.configRepository = configRepository;
    this.uuidGenerator = uuidGenerator;
    this.workspaceService = workspaceService;
  }

  public CheckOperationRead checkOperation(final OperatorConfiguration operationCheck) {
    try {
      validateOperation(operationCheck);
    } catch (final IllegalArgumentException e) {
      return new CheckOperationRead().status(StatusEnum.FAILED)
          .message(e.getMessage());
    }
    return new CheckOperationRead().status(StatusEnum.SUCCEEDED);
  }

  @SuppressWarnings({"PMD.PreserveStackTrace"})
  public OperationRead createOperation(final OperationCreate operationCreate)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID operationId = uuidGenerator.get();
    final StandardWorkspace workspace;
    try {
      workspace = workspaceService.getWorkspaceWithSecrets(operationCreate.getWorkspaceId(), false);
    } catch (io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException("WORKSPACE", operationCreate.getWorkspaceId().toString());
    }
    final StandardSyncOperation standardSyncOperation = toStandardSyncOperation(operationCreate, workspace)
        .withOperationId(operationId);
    return persistOperation(standardSyncOperation);
  }

  private StandardSyncOperation toStandardSyncOperation(final OperationCreate operationCreate, final StandardWorkspace workspace) {
    final StandardSyncOperation standardSyncOperation = new StandardSyncOperation()
        .withWorkspaceId(operationCreate.getWorkspaceId())
        .withName(operationCreate.getName())
        .withOperatorType(Enums.convertTo(operationCreate.getOperatorConfiguration().getOperatorType(), OperatorType.class))
        .withTombstone(false);
    OperationsConverter.populateOperatorConfigFromApi(operationCreate.getOperatorConfiguration(), standardSyncOperation, workspace);
    return standardSyncOperation;
  }

  private void validateOperation(final OperatorConfiguration operatorConfiguration) {
    if (io.airbyte.api.model.generated.OperatorType.WEBHOOK.equals(operatorConfiguration.getOperatorType())) {
      Preconditions.checkArgument(operatorConfiguration.getWebhook() != null);
    }
  }

  @SuppressWarnings({"PMD.PreserveStackTrace"})
  public OperationRead updateOperation(final OperationUpdate operationUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationUpdate.getOperationId());
    final StandardWorkspace workspace;
    try {
      workspace = workspaceService.getWorkspaceWithSecrets(standardSyncOperation.getWorkspaceId(), false);
    } catch (io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException("WORKSPACE", standardSyncOperation.getWorkspaceId().toString());
    }
    return persistOperation(updateOperation(operationUpdate, standardSyncOperation, workspace));
  }

  public static StandardSyncOperation updateOperation(final OperationUpdate operationUpdate,
                                                      final StandardSyncOperation standardSyncOperation,
                                                      final StandardWorkspace workspace) {
    standardSyncOperation
        .withName(operationUpdate.getName());
    OperationsConverter.populateOperatorConfigFromApi(operationUpdate.getOperatorConfiguration(), standardSyncOperation, workspace);
    return standardSyncOperation;
  }

  private OperationRead persistOperation(final StandardSyncOperation standardSyncOperation)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    configRepository.writeStandardSyncOperation(standardSyncOperation);
    return buildOperationRead(standardSyncOperation.getOperationId());
  }

  public OperationReadList listOperationsForConnection(final ConnectionIdRequestBody connectionIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<OperationRead> operationReads = Lists.newArrayList();
    final StandardSync standardSync = configRepository.getStandardSync(connectionIdRequestBody.getConnectionId());
    for (final UUID operationId : standardSync.getOperationIds()) {
      final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationId);
      if (standardSyncOperation.getTombstone() != null && standardSyncOperation.getTombstone()) {
        continue;
      }
      operationReads.add(buildOperationRead(standardSyncOperation));
    }
    return new OperationReadList().operations(operationReads);
  }

  public OperationRead getOperation(final OperationIdRequestBody operationIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildOperationRead(operationIdRequestBody.getOperationId());
  }

  public void deleteOperationsForConnection(final ConnectionIdRequestBody connectionIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync standardSync = configRepository.getStandardSync(connectionIdRequestBody.getConnectionId());
    deleteOperationsForConnection(standardSync, standardSync.getOperationIds());
  }

  public void deleteOperationsForConnection(final UUID connectionId, final List<UUID> deleteOperationIds)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync standardSync = configRepository.getStandardSync(connectionId);
    deleteOperationsForConnection(standardSync, deleteOperationIds);
  }

  public void deleteOperationsForConnection(final StandardSync standardSync, final List<UUID> deleteOperationIds)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<UUID> operationIds = new ArrayList<>(standardSync.getOperationIds());
    for (final UUID operationId : deleteOperationIds) {
      operationIds.remove(operationId);
      boolean sharedOperation = false;
      for (final StandardSync sync : configRepository.listStandardSyncsUsingOperation(operationId)) {
        // Check if other connections are using the same operation
        if (!sync.getConnectionId().equals(standardSync.getConnectionId())) {
          sharedOperation = true;
          break;
        }
      }
      if (!sharedOperation) {
        removeOperation(operationId);
      }
    }

    configRepository.updateConnectionOperationIds(standardSync.getConnectionId(), new HashSet<>(operationIds));
  }

  public void deleteOperation(final OperationIdRequestBody operationIdRequestBody)
      throws IOException {
    final UUID operationId = operationIdRequestBody.getOperationId();
    configRepository.deleteStandardSyncOperation(operationId);
  }

  private void removeOperation(final UUID operationId) throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationId);
    if (standardSyncOperation != null) {
      standardSyncOperation.withTombstone(true);
      persistOperation(standardSyncOperation);
    } else {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC_OPERATION, operationId.toString());
    }
  }

  private OperationRead buildOperationRead(final UUID operationId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationId);
    if (standardSyncOperation != null) {
      return buildOperationRead(standardSyncOperation);
    } else {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC_OPERATION, operationId.toString());
    }
  }

  private static OperationRead buildOperationRead(final StandardSyncOperation standardSyncOperation) {
    return OperationsConverter.operationReadFromPersistedOperation(standardSyncOperation);
  }

}
