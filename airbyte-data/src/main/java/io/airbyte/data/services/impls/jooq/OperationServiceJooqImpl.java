/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.OPERATION;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.select;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.OperationService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;

@Singleton
public class OperationServiceJooqImpl implements OperationService {

  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public OperationServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Get sync operation.
   *
   * @param operationId operation id
   * @return sync operation
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public StandardSyncOperation getStandardSyncOperation(UUID operationId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listStandardSyncOperationQuery(Optional.of(operationId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC_OPERATION, operationId));
  }

  /**
   * Write standard sync operation.
   *
   * @param standardSyncOperation standard sync operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void writeStandardSyncOperation(StandardSyncOperation standardSyncOperation)
      throws IOException {
    database.transaction(ctx -> {
      writeStandardSyncOperation(Collections.singletonList(standardSyncOperation), ctx);
      return null;
    });
  }

  /**
   * List standard sync operations.
   *
   * @return standard sync operations.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<StandardSyncOperation> listStandardSyncOperations() throws IOException {
    return listStandardSyncOperationQuery(Optional.empty()).toList();
  }

  /**
   * Updates {@link io.airbyte.db.instance.configs.jooq.generated.tables.ConnectionOperation} records
   * for the given {@code connectionId}.
   *
   * @param connectionId ID of the associated connection to update operations for
   * @param newOperationIds Set of all operationIds that should be associated to the connection
   * @throws IOException - exception while interacting with the db
   */
  @Override
  public void updateConnectionOperationIds(UUID connectionId, Set<UUID> newOperationIds)
      throws IOException {
    database.transaction(ctx -> {
      final Set<UUID> existingOperationIds = ctx
          .selectFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
          .fetchSet(CONNECTION_OPERATION.OPERATION_ID);

      final Set<UUID> existingOperationIdsToKeep = Sets.intersection(existingOperationIds, newOperationIds);

      // DELETE existing connection_operation records that aren't in the input list
      final Set<UUID> operationIdsToDelete = Sets.difference(existingOperationIds, existingOperationIdsToKeep);

      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
          .and(CONNECTION_OPERATION.OPERATION_ID.in(operationIdsToDelete))
          .execute();

      // INSERT connection_operation records that are in the input list and don't yet exist
      final Set<UUID> operationIdsToAdd = Sets.difference(newOperationIds, existingOperationIdsToKeep);

      operationIdsToAdd.forEach(operationId -> ctx
          .insertInto(CONNECTION_OPERATION)
          .columns(CONNECTION_OPERATION.ID, CONNECTION_OPERATION.CONNECTION_ID, CONNECTION_OPERATION.OPERATION_ID)
          .values(UUID.randomUUID(), connectionId, operationId)
          .execute());

      return null;
    });
  }

  /**
   * Delete standard sync operation.
   *
   * @param standardSyncOperationId standard sync operation id
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void deleteStandardSyncOperation(UUID standardSyncOperationId) throws IOException {
    database.transaction(ctx -> {
      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.OPERATION_ID.eq(standardSyncOperationId)).execute();
      ctx.update(OPERATION)
          .set(OPERATION.UPDATED_AT, OffsetDateTime.now())
          .set(OPERATION.TOMBSTONE, true)
          .where(OPERATION.ID.eq(standardSyncOperationId)).execute();
      return null;
    });
  }

  private Stream<StandardSyncOperation> listStandardSyncOperationQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(OPERATION);
      if (configId.isPresent()) {
        return query.where(OPERATION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    return result.map(OperationServiceJooqImpl::buildStandardSyncOperation).stream();
  }

  private void writeStandardSyncOperation(final List<StandardSyncOperation> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((standardSyncOperation) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(OPERATION)
          .where(OPERATION.ID.eq(standardSyncOperation.getOperationId())));

      if (isExistingConfig) {
        ctx.update(OPERATION)
            .set(OPERATION.ID, standardSyncOperation.getOperationId())
            .set(OPERATION.WORKSPACE_ID, standardSyncOperation.getWorkspaceId())
            .set(OPERATION.NAME, standardSyncOperation.getName())
            .set(OPERATION.OPERATOR_TYPE, Enums.toEnum(standardSyncOperation.getOperatorType().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType.class).orElseThrow())
            .set(OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorWebhook())))
            .set(OPERATION.TOMBSTONE, standardSyncOperation.getTombstone() != null && standardSyncOperation.getTombstone())
            .set(OPERATION.UPDATED_AT, timestamp)
            .where(OPERATION.ID.eq(standardSyncOperation.getOperationId()))
            .execute();

      } else {
        ctx.insertInto(OPERATION)
            .set(OPERATION.ID, standardSyncOperation.getOperationId())
            .set(OPERATION.WORKSPACE_ID, standardSyncOperation.getWorkspaceId())
            .set(OPERATION.NAME, standardSyncOperation.getName())
            .set(OPERATION.OPERATOR_TYPE, Enums.toEnum(standardSyncOperation.getOperatorType().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType.class).orElseThrow())
            .set(OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.getOperatorWebhook())))
            .set(OPERATION.TOMBSTONE, standardSyncOperation.getTombstone() != null && standardSyncOperation.getTombstone())
            .set(OPERATION.CREATED_AT, timestamp)
            .set(OPERATION.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  private static StandardSyncOperation buildStandardSyncOperation(final Record record) {
    return new StandardSyncOperation()
        .withOperationId(record.get(OPERATION.ID))
        .withName(record.get(OPERATION.NAME))
        .withWorkspaceId(record.get(OPERATION.WORKSPACE_ID))
        .withOperatorType(Enums.toEnum(record.get(OPERATION.OPERATOR_TYPE, String.class), OperatorType.class).orElseThrow())
        .withOperatorWebhook(record.get(OPERATION.OPERATOR_WEBHOOK) == null ? null
            : Jsons.deserialize(record.get(OPERATION.OPERATOR_WEBHOOK).data(), OperatorWebhook.class))
        .withTombstone(record.get(OPERATION.TOMBSTONE));
  }

}
