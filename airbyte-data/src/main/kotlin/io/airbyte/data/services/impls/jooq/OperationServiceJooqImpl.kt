/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Sets
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSyncOperation
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.OperationService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.OperatorType
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Stream

@Singleton
class OperationServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : OperationService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Get sync operation.
     *
     * @param operationId operation id
     * @return sync operation
     * @throws JsonValidationException if the workspace is or contains invalid json
     * @throws ConfigNotFoundException if the config does not exist
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getStandardSyncOperation(operationId: UUID): StandardSyncOperation =
      listStandardSyncOperationQuery(Optional.of(operationId))
        .findFirst()
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.STANDARD_SYNC_OPERATION,
            operationId,
          )
        }

    /**
     * Write standard sync operation.
     *
     * @param standardSyncOperation standard sync operation.
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun writeStandardSyncOperation(standardSyncOperation: StandardSyncOperation) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeStandardSyncOperation(listOf(standardSyncOperation), ctx)
        null
      }
    }

    /**
     * List standard sync operations.
     *
     * @return standard sync operations.
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listStandardSyncOperations(): List<StandardSyncOperation> = listStandardSyncOperationQuery(Optional.empty()).toList()

    /**
     * Updates [io.airbyte.db.instance.configs.jooq.generated.tables.ConnectionOperation] records
     * for the given `connectionId`.
     *
     * @param connectionId ID of the associated connection to update operations for
     * @param newOperationIds Set of all operationIds that should be associated to the connection
     * @throws IOException - exception while interacting with the db
     */
    @Throws(IOException::class)
    override fun updateConnectionOperationIds(
      connectionId: UUID,
      newOperationIds: Set<UUID>,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        val existingOperationIds =
          ctx
            .selectFrom(Tables.CONNECTION_OPERATION)
            .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
            .fetchSet(Tables.CONNECTION_OPERATION.OPERATION_ID)
        val existingOperationIdsToKeep: Set<UUID?> =
          Sets.intersection(existingOperationIds, newOperationIds)

        // DELETE existing connection_operation records that aren't in the input list
        val operationIdsToDelete: Set<UUID?> =
          Sets.difference(existingOperationIds, existingOperationIdsToKeep)

        ctx
          .deleteFrom(Tables.CONNECTION_OPERATION)
          .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
          .and(Tables.CONNECTION_OPERATION.OPERATION_ID.`in`(operationIdsToDelete))
          .execute()

        // INSERT connection_operation records that are in the input list and don't yet exist
        val operationIdsToAdd: Set<UUID> =
          Sets.difference(newOperationIds, existingOperationIdsToKeep)

        operationIdsToAdd.forEach(
          Consumer { operationId: UUID ->
            ctx
              .insertInto(Tables.CONNECTION_OPERATION)
              .columns(
                Tables.CONNECTION_OPERATION.ID,
                Tables.CONNECTION_OPERATION.CONNECTION_ID,
                Tables.CONNECTION_OPERATION.OPERATION_ID,
              ).values(UUID.randomUUID(), connectionId, operationId)
              .execute()
          },
        )
        null
      }
    }

    /**
     * Delete standard sync operation.
     *
     * @param standardSyncOperationId standard sync operation id
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun deleteStandardSyncOperation(standardSyncOperationId: UUID) {
      database.transaction<Any?> { ctx: DSLContext ->
        ctx
          .deleteFrom(Tables.CONNECTION_OPERATION)
          .where(Tables.CONNECTION_OPERATION.OPERATION_ID.eq(standardSyncOperationId))
          .execute()
        ctx
          .update(Tables.OPERATION)
          .set(
            Tables.OPERATION.UPDATED_AT,
            OffsetDateTime.now(),
          ).set(Tables.OPERATION.TOMBSTONE, true)
          .where(Tables.OPERATION.ID.eq(standardSyncOperationId))
          .execute()
        null
      }
    }

    @Throws(IOException::class)
    private fun listStandardSyncOperationQuery(configId: Optional<UUID>): Stream<StandardSyncOperation> {
      val result =
        database.query { ctx: DSLContext ->
          val query = ctx.select(DSL.asterisk()).from(Tables.OPERATION)
          if (configId.isPresent) {
            return@query query.where(Tables.OPERATION.ID.eq(configId.get())).fetch()
          }
          query.fetch()
        }

      return result.map { record: Record -> buildStandardSyncOperation(record) }.stream()
    }

    private fun writeStandardSyncOperation(
      configs: List<StandardSyncOperation>,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      configs.forEach(
        Consumer { standardSyncOperation: StandardSyncOperation ->
          val isExistingConfig =
            ctx.fetchExists(
              DSL
                .select()
                .from(Tables.OPERATION)
                .where(Tables.OPERATION.ID.eq(standardSyncOperation.operationId)),
            )
          if (isExistingConfig) {
            ctx
              .update(Tables.OPERATION)
              .set(Tables.OPERATION.ID, standardSyncOperation.operationId)
              .set(Tables.OPERATION.WORKSPACE_ID, standardSyncOperation.workspaceId)
              .set(Tables.OPERATION.NAME, standardSyncOperation.name)
              .set(Tables.OPERATION.OPERATOR_TYPE, standardSyncOperation.operatorType.value().toEnum<OperatorType>()!!)
              .set(Tables.OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.operatorWebhook)))
              .set(Tables.OPERATION.TOMBSTONE, standardSyncOperation.tombstone != null && standardSyncOperation.tombstone)
              .set(Tables.OPERATION.UPDATED_AT, timestamp)
              .where(Tables.OPERATION.ID.eq(standardSyncOperation.operationId))
              .execute()
          } else {
            ctx
              .insertInto(Tables.OPERATION)
              .set(Tables.OPERATION.ID, standardSyncOperation.operationId)
              .set(Tables.OPERATION.WORKSPACE_ID, standardSyncOperation.workspaceId)
              .set(Tables.OPERATION.NAME, standardSyncOperation.name)
              .set(Tables.OPERATION.OPERATOR_TYPE, standardSyncOperation.operatorType.value().toEnum<OperatorType>()!!)
              .set(Tables.OPERATION.OPERATOR_WEBHOOK, JSONB.valueOf(Jsons.serialize(standardSyncOperation.operatorWebhook)))
              .set(Tables.OPERATION.TOMBSTONE, standardSyncOperation.tombstone != null && standardSyncOperation.tombstone)
              .set(Tables.OPERATION.CREATED_AT, timestamp)
              .set(Tables.OPERATION.UPDATED_AT, timestamp)
              .execute()
          }
        },
      )
    }

    companion object {
      private fun buildStandardSyncOperation(record: Record): StandardSyncOperation =
        StandardSyncOperation()
          .withOperationId(record.get(Tables.OPERATION.ID))
          .withName(record.get(Tables.OPERATION.NAME))
          .withWorkspaceId(record.get(Tables.OPERATION.WORKSPACE_ID))
          .withOperatorType(
            record
              .get(
                Tables.OPERATION.OPERATOR_TYPE,
                String::class.java,
              ).toEnum<StandardSyncOperation.OperatorType>()!!,
          ).withOperatorWebhook(
            if (record.get(Tables.OPERATION.OPERATOR_WEBHOOK) == null) {
              null
            } else {
              Jsons.deserialize(
                record.get(Tables.OPERATION.OPERATOR_WEBHOOK).data(),
                OperatorWebhook::class.java,
              )
            },
          ).withTombstone(record.get(Tables.OPERATION.TOMBSTONE))
    }
  }
